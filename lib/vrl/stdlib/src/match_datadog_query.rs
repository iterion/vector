use vrl::prelude::*;

use datadog_search_syntax::{
    normalize_fields, parse, BooleanType, Comparison, ComparisonValue, Field, QueryNode,
};
use dyn_clone::DynClone;
use lookup_lib::{parser::parse_lookup, LookupBuf};
use regex::Regex;
use std::borrow::Cow;
use vrl::prelude::fmt::Formatter;

#[derive(Clone, Copy, Debug)]
pub struct MatchDatadogQuery;

impl Function for MatchDatadogQuery {
    fn identifier(&self) -> &'static str {
        "match_datadog_query"
    }

    fn examples(&self) -> &'static [Example] {
        &[
            Example {
                title: "OR query",
                source: r#"match_datadog_query({"message": "contains this and that"}, "this OR that")"#,
                result: Ok("true"),
            },
            Example {
                title: "AND query",
                source: r#"match_datadog_query({"message": "contains only this"}, "this AND that")"#,
                result: Ok("false"),
            },
            Example {
                title: "Facet wildcard",
                source: r#"match_datadog_query({"custom": {"name": "vector"}}, "@name:vec*")"#,
                result: Ok("true"),
            },
            Example {
                title: "Tag range",
                source: r#"match_datadog_query({"tags": ["a:x", "b:y", "c:z"]}, s'b:["x" TO "z"]')"#,
                result: Ok("true"),
            },
        ]
    }

    fn compile(
        &self,
        _state: &state::Compiler,
        _ctx: &FunctionCompileContext,
        mut arguments: ArgumentList,
    ) -> Compiled {
        let value = arguments.required("value");
        let query_value = arguments.required_literal("query")?.to_value();

        // Query should always be a string.
        let query = query_value
            .try_bytes_utf8_lossy()
            .expect("datadog search query should be a UTF8 string");

        // Compile the Datadog search query to AST.
        let node = parse(&query).map_err(|e| {
            Box::new(ExpressionError::from(e.to_string())) as Box<dyn DiagnosticError>
        })?;

        // Build the matcher function that accepts a VRL event value. This will parse the `node`
        // at boot-time and return a boxed func that contains just the logic required to match a
        // VRL `Value` against the Datadog Search Syntax literal.
        let matcher = build_matcher(&node);

        Ok(Box::new(MatchDatadogQueryFn { value, matcher }))
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[
            Parameter {
                keyword: "value",
                kind: kind::OBJECT,
                required: true,
            },
            Parameter {
                keyword: "query",
                kind: kind::BYTES,
                required: true,
            },
        ]
    }
}

#[derive(Debug, Clone)]
struct MatchDatadogQueryFn {
    value: Box<dyn Expression>,
    matcher: Box<dyn Matcher>,
}

impl Expression for MatchDatadogQueryFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let value = self.value.resolve(ctx)?.try_object()?;

        // Provide the current VRL event `Value` to the matcher function to determine
        // whether the data matches the given Datadog Search syntax literal.
        Ok(self.matcher.run(&Value::Object(value)).into())
    }

    fn type_def(&self, _state: &state::Compiler) -> TypeDef {
        type_def()
    }
}

fn type_def() -> TypeDef {
    TypeDef::new().infallible().boolean()
}

/// A `Matcher` contains a single method ("run") which is passed a VRL value, and returns
/// `true` if matches an expression determined by the Matcher implementor.
trait Matcher: DynClone + std::fmt::Debug + Send + Sync {
    fn run(&self, obj: &Value) -> bool;
}

dyn_clone::clone_trait_object!(Matcher);

/// Container for holding a thread-safe function type that can receive a VRL
/// `Value`, and return true/false some internal expression.
#[derive(Clone)]
struct Match<T: Fn(&Value) -> bool + Send + Sync + Clone> {
    func: T,
}

impl<T: Fn(&Value) -> bool + Send + Sync + Clone> Match<T> {
    fn boxed(func: T) -> Box<Self> {
        Box::new(Self { func })
    }
}

impl<T: Fn(&Value) -> bool + Send + Sync + Clone> Matcher for Match<T> {
    fn run(&self, obj: &Value) -> bool {
        (self.func)(obj)
    }
}

impl<T: Fn(&Value) -> bool + Send + Sync + Clone> std::fmt::Debug for Match<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Datadog matcher fn")
    }
}

/// Returns a vector of lookup fields based on the normalized field type.
fn attr_to_lookup_fields<T: AsRef<str>>(attr: T) -> Vec<(Field, LookupBuf)> {
    normalize_fields(attr)
        .into_iter()
        .filter_map(|field| lookup_field(&field).map(move |buf| (field, buf)))
        .collect()
}

fn resolve_value(buf: LookupBuf, match_fn: Box<dyn Matcher>) -> Box<dyn Matcher> {
    let func = move |obj: &Value| {
        // Get the value by path, or return early with `false` if it doesn't exist.
        let value = match obj.get_by_path(&buf) {
            Some(v) => v,
            _ => return false,
        };

        match_fn.run(value)
    };

    Match::boxed(func)
}

impl Matcher for bool {
    fn run(&self, _obj: &Value) -> bool {
        *self
    }
}

fn build_not(func: Box<dyn Matcher>) -> Box<dyn Matcher> {
    Match::boxed(move |obj| !func.run(obj))
}

fn build_exists(field: Field) -> Box<dyn Matcher> {
    match field {
        // Tags need to check the element value.
        Field::Tag(tag) => {
            let starts_with = format!("{}:", tag);

            Match::boxed(move |obj| match obj {
                Value::Array(v) => v.iter().any(|v| {
                    let str_value = string_value(v);

                    // The tag matches using either 'key' or 'key:value' syntax.
                    str_value == tag || str_value.starts_with(&starts_with)
                }),
                _ => false,
            })
        }
        // Literal field 'tags' needs to be compared by key.
        Field::Reserved(f) if f == "tags" => Match::boxed(|obj| match obj {
            Value::Array(v) => v.iter().any(|v| v == obj),
            _ => false,
        }),
        // Other field types have already resolved at this point, so just return true.
        _ => Box::new(true),
    }
}

/// Returns true if the provided VRL `Value` matches the `to_match` string.
fn build_equals(field: Field, to_match: String) -> Box<dyn Matcher> {
    match field {
        // Default fields are compared by word boundary.
        Field::Default(_) => {
            let re = word_regex(&to_match);

            Match::boxed(move |value| match value {
                Value::Bytes(val) => re.is_match(&String::from_utf8_lossy(val)),
                _ => false,
            })
        }
        // A literal "tags" field should match by key.
        Field::Reserved(f) if f == "tags" => Match::boxed(move |value| match value {
            Value::Array(v) => {
                v.contains(&Value::Bytes(Bytes::copy_from_slice(to_match.as_bytes())))
            }
            _ => false,
        }),
        // Individual tags are compared by element key:value.
        Field::Tag(tag) => {
            let value_bytes = Value::Bytes(format!("{}:{}", tag, to_match).into());

            Match::boxed(move |value| match value {
                Value::Array(v) => v.contains(&value_bytes),
                _ => false,
            })
        }
        // Everything else is matched by string equality.
        _ => Match::boxed(move |value| string_value(value) == to_match),
    }
}

/// Returns a boxed `Matcher` that compares a VRL `Value` with a Datadog Search Syntax value.
/// Compatible with all value comparison operators. Handles strings and numeric values.
fn compare(
    field: Field,
    comparator: Comparison,
    comparison_value: ComparisonValue,
) -> Box<dyn Matcher> {
    let rhs = Cow::from(comparison_value.to_string());

    match field {
        // Facets are compared numerically if the value is numeric, or as strings otherwise.
        Field::Facet(_) => {
            Match::boxed(move |value| match (value, &comparison_value) {
                // Integers.
                (Value::Integer(lhs), ComparisonValue::Integer(rhs)) => match comparator {
                    Comparison::Lt => *lhs < *rhs,
                    Comparison::Lte => *lhs <= *rhs,
                    Comparison::Gt => *lhs > *rhs,
                    Comparison::Gte => *lhs >= *rhs,
                },
                // Floats.
                (Value::Float(lhs), ComparisonValue::Float(rhs)) => {
                    let lhs = lhs.into_inner();
                    match comparator {
                        Comparison::Lt => lhs < *rhs,
                        Comparison::Lte => lhs <= *rhs,
                        Comparison::Gt => lhs > *rhs,
                        Comparison::Gte => lhs >= *rhs,
                    }
                }
                // Where the rhs is a string ref, the lhs is coerced into a string.
                (_, ComparisonValue::String(rhs)) => {
                    let lhs = string_value(value);
                    let rhs = Cow::from(rhs);

                    match comparator {
                        Comparison::Lt => lhs < rhs,
                        Comparison::Lte => lhs <= rhs,
                        Comparison::Gt => lhs > rhs,
                        Comparison::Gte => lhs >= rhs,
                    }
                }
                // Otherwise, compare directly as strings.
                _ => {
                    let lhs = string_value(value);

                    match comparator {
                        Comparison::Lt => lhs < rhs,
                        Comparison::Lte => lhs <= rhs,
                        Comparison::Gt => lhs > rhs,
                        Comparison::Gte => lhs >= rhs,
                    }
                }
            })
        }
        // Tag values need extracting by "key:value" to be compared.
        Field::Tag(_) => Match::boxed(move |value| match value {
            Value::Array(v) => v.iter().any(|v| match string_value(v).split_once(":") {
                Some((_, lhs)) => {
                    let lhs = Cow::from(lhs);

                    match comparator {
                        Comparison::Lt => lhs < rhs,
                        Comparison::Lte => lhs <= rhs,
                        Comparison::Gt => lhs > rhs,
                        Comparison::Gte => lhs >= rhs,
                    }
                }
                _ => false,
            }),
            _ => false,
        }),
        // All other tag types are compared by string.
        _ => Match::boxed(move |value| {
            let lhs = string_value(value);

            match comparator {
                Comparison::Lt => lhs < rhs,
                Comparison::Lte => lhs <= rhs,
                Comparison::Gt => lhs > rhs,
                Comparison::Gte => lhs >= rhs,
            }
        }),
    }
}

/// Returns a boxed `Matcher` that determines whether a VRL value falls within a provided
/// Datadog Search Syntax value range. Handles strings and numeric values.
fn range(
    field: Field,
    lower: ComparisonValue,
    lower_inclusive: bool,
    upper: ComparisonValue,
    upper_inclusive: bool,
) -> Box<dyn Matcher> {
    match (&lower, &upper) {
        // If both bounds are wildcards, just check that the field exists to catch the
        // special case for "tags".
        (ComparisonValue::Unbounded, ComparisonValue::Unbounded) => build_exists(field),
        // Unbounded lower.
        (ComparisonValue::Unbounded, _) => {
            let op = if upper_inclusive {
                Comparison::Lte
            } else {
                Comparison::Lt
            };

            compare(field, op, upper)
        }
        // Unbounded upper.
        (_, ComparisonValue::Unbounded) => {
            let op = if lower_inclusive {
                Comparison::Gte
            } else {
                Comparison::Gt
            };

            compare(field, op, lower)
        }
        // Definitive range.
        _ => {
            let lower_op = if lower_inclusive {
                Comparison::Gte
            } else {
                Comparison::Gt
            };

            let upper_op = if upper_inclusive {
                Comparison::Lte
            } else {
                Comparison::Lt
            };

            let lower_func = compare(field.clone(), lower_op, lower);
            let upper_func = compare(field, upper_op, upper);

            Match::boxed(move |value| lower_func.run(value) && upper_func.run(value))
        }
    }
}

/// Returns a boxed `Matcher`
fn wildcard_with_prefix(field: Field, prefix: String) -> Box<dyn Matcher> {
    match field {
        // Default fields are matched by word boundary.
        Field::Default(_) => {
            let re = word_regex(&format!("{}*", prefix));

            Match::boxed(move |value| re.is_match(&string_value(value)))
        }
        // Tags are recursed until a match is found.
        Field::Tag(tag) => {
            let starts_with = format!("{}:{}", tag, prefix);

            Match::boxed(move |value| match value {
                Value::Array(v) => v.iter().any(|v| string_value(v).starts_with(&starts_with)),
                _ => false,
            })
        }
        // All other field types are compared by complete value.
        _ => Match::boxed(move |value| string_value(value).starts_with(&prefix)),
    }
}

fn build_wildcard(field: Field, wildcard: &str) -> Box<dyn Matcher> {
    match field {
        Field::Default(_) => {
            let re = word_regex(wildcard);

            Match::boxed(move |value| re.is_match(&string_value(value)))
        }
        Field::Tag(tag) => {
            let re = wildcard_regex(&format!("{}:{}", tag, wildcard));

            Match::boxed(move |value| match value {
                Value::Array(v) => v.iter().any(|v| re.is_match(&string_value(v))),
                _ => false,
            })
        }
        _ => {
            let re = wildcard_regex(wildcard);

            Match::boxed(move |value| re.is_match(&string_value(value)))
        }
    }
}

fn any(queries: Vec<Box<dyn Matcher>>) -> Box<dyn Matcher> {
    let func = move |obj: &Value| queries.iter().any(|func| func.run(obj));
    Match::boxed(func)
}

fn all(queries: Vec<Box<dyn Matcher>>) -> Box<dyn Matcher> {
    let func = move |obj: &Value| queries.iter().all(|func| func.run(obj));
    Match::boxed(func)
}

fn build_matcher(node: &QueryNode) -> Box<dyn Matcher> {
    match node {
        QueryNode::MatchNoDocs => Box::new(false),
        QueryNode::MatchAllDocs => Box::new(true),
        QueryNode::AttributeExists { attr } => {
            let queries = attr_to_lookup_fields(attr)
                .into_iter()
                .map(|(field, buf)| resolve_value(buf, build_exists(field)))
                .collect::<Vec<_>>();

            any(queries)
        }
        QueryNode::AttributeMissing { attr } => {
            let queries = attr_to_lookup_fields(attr)
                .into_iter()
                .map(|(field, buf)| build_not(resolve_value(buf, build_exists(field))))
                .collect::<Vec<_>>();

            all(queries)
        }
        QueryNode::AttributeTerm { attr, value }
        | QueryNode::QuotedAttribute {
            attr,
            phrase: value,
        } => {
            let queries = attr_to_lookup_fields(attr)
                .into_iter()
                .map(|(field, buf)| resolve_value(buf, build_equals(field, value.clone())))
                .collect::<Vec<_>>();

            any(queries)
        }
        QueryNode::AttributeComparison {
            attr,
            comparator,
            value,
        } => {
            let queries = attr_to_lookup_fields(attr)
                .into_iter()
                .map(|(field, buf)| resolve_value(buf, compare(field, *comparator, value.clone())))
                .collect::<Vec<_>>();

            any(queries)
        }
        QueryNode::AttributePrefix { attr, prefix } => {
            let queries = attr_to_lookup_fields(attr)
                .into_iter()
                .map(|(field, buf)| resolve_value(buf, wildcard_with_prefix(field, prefix.clone())))
                .collect::<Vec<_>>();

            any(queries)
        }
        QueryNode::AttributeWildcard { attr, wildcard } => {
            let queries = attr_to_lookup_fields(attr)
                .into_iter()
                .map(|(field, buf)| resolve_value(buf, build_wildcard(field, wildcard)))
                .collect::<Vec<_>>();

            any(queries)
        }
        QueryNode::AttributeRange {
            attr,
            lower,
            lower_inclusive,
            upper,
            upper_inclusive,
        } => {
            let queries = attr_to_lookup_fields(attr)
                .into_iter()
                .map(|(field, buf)| {
                    resolve_value(
                        buf,
                        range(
                            field,
                            lower.clone(),
                            *lower_inclusive,
                            upper.clone(),
                            *upper_inclusive,
                        ),
                    )
                })
                .collect::<Vec<_>>();

            any(queries)
        }
        QueryNode::NegatedNode { node } => build_not(build_matcher(node)),
        QueryNode::Boolean { oper, nodes } => {
            let funcs = nodes.iter().map(build_matcher).collect::<Vec<_>>();

            match oper {
                BooleanType::And => all(funcs),
                BooleanType::Or => any(funcs),
            }
        }
    }
}

/// Returns compiled word boundary regex. Cached to avoid recompilation in hot paths.
fn word_regex(to_match: &str) -> Regex {
    Regex::new(&format!(
        r#"\b{}\b"#,
        regex::escape(to_match).replace("\\*", ".*")
    ))
    .expect("invalid wildcard regex")
}

/// Returns compiled wildcard regex. Cached to avoid recompilation in hot paths.
fn wildcard_regex(to_match: &str) -> Regex {
    Regex::new(&format!(
        "^{}$",
        regex::escape(to_match).replace("\\*", ".*")
    ))
    .expect("invalid wildcard regex")
}

/// If the provided field is a `Field::Tag`, will return a "tags" lookup buf. Otherwise,
/// parses the field and returns a lookup buf is the lookup itself is valid.
fn lookup_field(field: &Field) -> Option<LookupBuf> {
    match field {
        Field::Default(p) | Field::Reserved(p) | Field::Facet(p) => {
            Some(parse_lookup(p.as_str()).ok()?.into_buf())
        }
        Field::Tag(_) => Some(LookupBuf::from("tags")),
    }
}

/// Returns a string value from a VRL `Value`. This differs from the regular `Display`
/// implementation by treating Bytes values as special-- returning the UTF8 representation
/// instead of the raw control characters.
fn string_value(value: &Value) -> Cow<str> {
    match value {
        Value::Bytes(val) => String::from_utf8_lossy(val),
        _ => Cow::from(value.to_string()),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    test_function![
        match_datadog_query => MatchDatadogQuery;

        message_exists {
            args: func_args![value: value!({"message": "test message"}), query: "_exists_:message"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_message_exists {
            args: func_args![value: value!({"message": "test message"}), query: "NOT _exists_:message"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_message_exists {
            args: func_args![value: value!({"message": "test message"}), query: "-_exists_:message"],
            want: Ok(false),
            tdef: type_def(),
        }

        facet_exists {
            args: func_args![value: value!({"custom": {"a": "value" }}), query: "_exists_:@a"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_facet_exists {
            args: func_args![value: value!({"custom": {"a": "value" }}), query: "NOT _exists_:@a"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_facet_exists {
            args: func_args![value: value!({"custom": {"a": "value" }}), query: "-_exists_:@a"],
            want: Ok(false),
            tdef: type_def(),
        }

        tag_bare {
            args: func_args![value: value!({"tags": ["a","b","c"]}), query: "tags:a"],
            want: Ok(true),
            tdef: type_def(),
        }

        tag_bare_no_match {
            args: func_args![value: value!({"tags": ["a","b","c"]}), query: "tags:d"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_tag_bare {
            args: func_args![value: value!({"tags": ["a","b","c"]}), query: "NOT tags:a"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_tag_bare {
            args: func_args![value: value!({"tags": ["a","b","c"]}), query: "-tags:a"],
            want: Ok(false),
            tdef: type_def(),
        }

        tag_exists_bare {
            args: func_args![value: value!({"tags": ["a","b","c"]}), query: "_exists_:a"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_tag_exists_bare {
            args: func_args![value: value!({"tags": ["a","b","c"]}), query: "NOT _exists_:a"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_tag_exists_bare {
            args: func_args![value: value!({"tags": ["a","b","c"]}), query: "-_exists_:a"],
            want: Ok(false),
            tdef: type_def(),
        }

        tag_exists {
            args: func_args![value: value!({"tags": ["a:1","b:2","c:3"]}), query: "_exists_:a"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_tag_exists {
            args: func_args![value: value!({"tags": ["a:1","b:2","c:3"]}), query: "NOT _exists_:a"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_tag_exists {
            args: func_args![value: value!({"tags": ["a:1","b:2","c:3"]}), query: "-_exists_:a"],
            want: Ok(false),
            tdef: type_def(),
        }

        message_missing {
            args: func_args![value: value!({}), query: "_missing_:message"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_message_missing {
            args: func_args![value: value!({}), query: "NOT _missing_:message"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_message_missing {
            args: func_args![value: value!({}), query: "-_missing_:message"],
            want: Ok(false),
            tdef: type_def(),
        }

        facet_missing {
            args: func_args![value: value!({"custom": {"b": "value" }}), query: "_missing_:@a"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_facet_missing {
            args: func_args![value: value!({"custom": {"b": "value" }}), query: "NOT _missing_:@a"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_facet_missing {
            args: func_args![value: value!({"custom": {"b": "value" }}), query: "-_missing_:@a"],
            want: Ok(false),
            tdef: type_def(),
        }

        tag_bare_missing {
            args: func_args![value: value!({"tags": ["b","c"]}), query: "_missing_:a"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_tag_bare_missing {
            args: func_args![value: value!({"tags": ["b","c"]}), query: "NOT _missing_:a"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_tag_bare_missing {
            args: func_args![value: value!({"tags": ["b","c"]}), query: "-_missing_:a"],
            want: Ok(false),
            tdef: type_def(),
        }

        tag_missing {
            args: func_args![value: value!({"tags": ["b:1","c:2"]}), query: "_missing_:a"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_tag_missing {
            args: func_args![value: value!({"tags": ["b:1","c:2"]}), query: "NOT _missing_:a"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_tag_missing {
            args: func_args![value: value!({"tags": ["b:1","c:2"]}), query: "-_missing_:a"],
            want: Ok(false),
            tdef: type_def(),
        }

        equals_message {
            args: func_args![value: value!({"message": "match by word boundary"}), query: "match"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_equals_message {
            args: func_args![value: value!({"message": "match by word boundary"}), query: "NOT match"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_equals_message {
            args: func_args![value: value!({"message": "match by word boundary"}), query: "-match"],
            want: Ok(false),
            tdef: type_def(),
        }

        equals_message_no_match {
            args: func_args![value: value!({"message": "another value"}), query: "match"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_equals_message_no_match {
            args: func_args![value: value!({"message": "another value"}), query: "NOT match"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_equals_message_no_match {
            args: func_args![value: value!({"message": "another value"}), query: "-match"],
            want: Ok(true),
            tdef: type_def(),
        }

        equals_tag {
            args: func_args![value: value!({"tags": ["x:1", "y:2", "z:3"]}), query: "y:2"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_equals_tag {
            args: func_args![value: value!({"tags": ["x:1", "y:2", "z:3"]}), query: "NOT y:2"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_equals_tag {
            args: func_args![value: value!({"tags": ["x:1", "y:2", "z:3"]}), query: "-y:2"],
            want: Ok(false),
            tdef: type_def(),
        }

        equals_tag_no_match {
            args: func_args![value: value!({"tags": ["x:1", "y:2", "z:3"]}), query: "y:3"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_equals_tag_no_match {
            args: func_args![value: value!({"tags": ["x:1", "y:2", "z:3"]}), query: "NOT y:3"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_equals_tag_no_match {
            args: func_args![value: value!({"tags": ["x:1", "y:2", "z:3"]}), query: "-y:3"],
            want: Ok(true),
            tdef: type_def(),
        }

        equals_facet {
            args: func_args![value: value!({"custom": {"z": 1}}), query: "@z:1"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_equals_facet {
            args: func_args![value: value!({"custom": {"z": 1}}), query: "NOT @z:1"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_equals_facet {
            args: func_args![value: value!({"custom": {"z": 1}}), query: "-@z:1"],
            want: Ok(false),
            tdef: type_def(),
        }

        wildcard_prefix_message {
            args: func_args![value: value!({"message": "vector"}), query: "*tor"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_wildcard_prefix_message {
            args: func_args![value: value!({"message": "vector"}), query: "NOT *tor"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_wildcard_prefix_message {
            args: func_args![value: value!({"message": "vector"}), query: "-*tor"],
            want: Ok(false),
            tdef: type_def(),
        }

        wildcard_prefix_message_no_match {
            args: func_args![value: value!({"message": "torvec"}), query: "*tor"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_wildcard_prefix_message_no_match {
            args: func_args![value: value!({"message": "torvec"}), query: "NOT *tor"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_wildcard_prefix_message_no_match {
            args: func_args![value: value!({"message": "torvec"}), query: "-*tor"],
            want: Ok(true),
            tdef: type_def(),
        }

        wildcard_prefix_tag {
            args: func_args![value: value!({"tags": ["a:vector"]}), query: "a:*tor"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_wildcard_prefix_tag {
            args: func_args![value: value!({"tags": ["a:vector"]}), query: "NOT a:*tor"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_wildcard_prefix_tag {
            args: func_args![value: value!({"tags": ["a:vector"]}), query: "-a:*tor"],
            want: Ok(false),
            tdef: type_def(),
        }

        wildcard_prefix_tag_no_match {
            args: func_args![value: value!({"tags": ["b:vector"]}), query: "a:*tor"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_wildcard_prefix_tag_no_match {
            args: func_args![value: value!({"tags": ["b:vector"]}), query: "NOT a:*tor"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_wildcard_prefix_tag_no_match {
            args: func_args![value: value!({"tags": ["b:vector"]}), query: "-a:*tor"],
            want: Ok(true),
            tdef: type_def(),
        }

        wildcard_prefix_facet {
            args: func_args![value: value!({"custom": {"a": "vector"}}), query: "@a:*tor"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_wildcard_prefix_facet {
            args: func_args![value: value!({"custom": {"a": "vector"}}), query: "NOT @a:*tor"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_wildcard_prefix_facet {
            args: func_args![value: value!({"custom": {"a": "vector"}}), query: "-@a:*tor"],
            want: Ok(false),
            tdef: type_def(),
        }

        wildcard_prefix_facet_no_match {
            args: func_args![value: value!({"custom": {"b": "vector"}}), query: "@a:*tor"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_wildcard_prefix_facet_no_match {
            args: func_args![value: value!({"custom": {"b": "vector"}}), query: "NOT @a:*tor"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_wildcard_prefix_facet_no_match {
            args: func_args![value: value!({"custom": {"b": "vector"}}), query: "-@a:*tor"],
            want: Ok(true),
            tdef: type_def(),
        }

        wildcard_suffix_message {
            args: func_args![value: value!({"message": "vector"}), query: "vec*"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_wildcard_suffix_message {
            args: func_args![value: value!({"message": "vector"}), query: "NOT vec*"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_wildcard_suffix_message {
            args: func_args![value: value!({"message": "vector"}), query: "-vec*"],
            want: Ok(false),
            tdef: type_def(),
        }

        wildcard_suffix_message_no_match {
            args: func_args![value: value!({"message": "torvec"}), query: "vec*"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_wildcard_suffix_message_no_match {
            args: func_args![value: value!({"message": "torvec"}), query: "NOT vec*"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_wildcard_suffix_message_no_match {
            args: func_args![value: value!({"message": "torvec"}), query: "-vec*"],
            want: Ok(true),
            tdef: type_def(),
        }

        wildcard_suffix_tag {
            args: func_args![value: value!({"tags": ["a:vector"]}), query: "a:vec*"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_wildcard_suffix_tag {
            args: func_args![value: value!({"tags": ["a:vector"]}), query: "NOT a:vec*"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_wildcard_suffix_tag {
            args: func_args![value: value!({"tags": ["a:vector"]}), query: "-a:vec*"],
            want: Ok(false),
            tdef: type_def(),
        }

        wildcard_suffix_tag_no_match {
            args: func_args![value: value!({"tags": ["b:vector"]}), query: "a:vec*"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_wildcard_suffix_tag_no_match {
            args: func_args![value: value!({"tags": ["b:vector"]}), query: "NOT a:vec*"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_wildcard_suffix_tag_no_match {
            args: func_args![value: value!({"tags": ["b:vector"]}), query: "-a:vec*"],
            want: Ok(true),
            tdef: type_def(),
        }

        wildcard_suffix_facet {
            args: func_args![value: value!({"custom": {"a": "vector"}}), query: "@a:vec*"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_wildcard_suffix_facet {
            args: func_args![value: value!({"custom": {"a": "vector"}}), query: "NOT @a:vec*"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_wildcard_suffix_facet {
            args: func_args![value: value!({"custom": {"a": "vector"}}), query: "-@a:vec*"],
            want: Ok(false),
            tdef: type_def(),
        }

        wildcard_suffix_facet_no_match {
            args: func_args![value: value!({"custom": {"b": "vector"}}), query: "@a:vec*"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_wildcard_suffix_facet_no_match {
            args: func_args![value: value!({"custom": {"b": "vector"}}), query: "NOT @a:vec*"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_wildcard_suffix_facet_no_match {
            args: func_args![value: value!({"custom": {"b": "vector"}}), query: "-@a:vec*"],
            want: Ok(true),
            tdef: type_def(),
        }

        wildcard_multiple_message {
            args: func_args![value: value!({"message": "vector"}), query: "v*c*r"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_wildcard_multiple_message {
            args: func_args![value: value!({"message": "vector"}), query: "NOT v*c*r"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_wildcard_multiple_message {
            args: func_args![value: value!({"message": "vector"}), query: "-v*c*r"],
            want: Ok(false),
            tdef: type_def(),
        }

        wildcard_multiple_message_no_match {
            args: func_args![value: value!({"message": "torvec"}), query: "v*c*r"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_wildcard_multiple_message_no_match {
            args: func_args![value: value!({"message": "torvec"}), query: "NOT v*c*r"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_wildcard_multiple_message_no_match {
            args: func_args![value: value!({"message": "torvec"}), query: "-v*c*r"],
            want: Ok(true),
            tdef: type_def(),
        }

        wildcard_multiple_tag {
            args: func_args![value: value!({"tags": ["a:vector"]}), query: "a:v*c*r"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_wildcard_multiple_tag {
            args: func_args![value: value!({"tags": ["a:vector"]}), query: "NOT a:v*c*r"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_wildcard_multiple_tag {
            args: func_args![value: value!({"tags": ["a:vector"]}), query: "-a:v*c*r"],
            want: Ok(false),
            tdef: type_def(),
        }

        wildcard_multiple_tag_no_match {
            args: func_args![value: value!({"tags": ["b:vector"]}), query: "a:v*c*r"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_wildcard_multiple_tag_no_match {
            args: func_args![value: value!({"tags": ["b:vector"]}), query: "NOT a:v*c*r"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_wildcard_multiple_tag_no_match {
            args: func_args![value: value!({"tags": ["b:vector"]}), query: "-a:v*c*r"],
            want: Ok(true),
            tdef: type_def(),
        }

        wildcard_multiple_facet {
            args: func_args![value: value!({"custom": {"a": "vector"}}), query: "@a:v*c*r"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_wildcard_multiple_facet {
            args: func_args![value: value!({"custom": {"a": "vector"}}), query: "NOT @a:v*c*r"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_wildcard_multiple_facet {
            args: func_args![value: value!({"custom": {"a": "vector"}}), query: "-@a:v*c*r"],
            want: Ok(false),
            tdef: type_def(),
        }

        wildcard_multiple_facet_no_match {
            args: func_args![value: value!({"custom": {"b": "vector"}}), query: "@a:v*c*r"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_wildcard_multiple_facet_no_match {
            args: func_args![value: value!({"custom": {"b": "vector"}}), query: "NOT @a:v*c*r"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_wildcard_multiple_facet_no_match {
            args: func_args![value: value!({"custom": {"b": "vector"}}), query: "-@a:v*c*r"],
            want: Ok(true),
            tdef: type_def(),
        }

        range_message_unbounded {
            args: func_args![value: value!({"message": "1"}), query: "[* TO *]"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_message_unbounded {
            args: func_args![value: value!({"message": "1"}), query: "NOT [* TO *]"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_message_unbounded {
            args: func_args![value: value!({"message": "1"}), query: "-[* TO *]"],
            want: Ok(false),
            tdef: type_def(),
        }

        range_message_lower_bound {
            args: func_args![value: value!({"message": "400"}), query: "[4 TO *]"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_message_lower_bound {
            args: func_args![value: value!({"message": "400"}), query: "NOT [4 TO *]"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_message_lower_bound {
            args: func_args![value: value!({"message": "400"}), query: "-[4 TO *]"],
            want: Ok(false),
            tdef: type_def(),
        }

        range_message_lower_bound_no_match {
            args: func_args![value: value!({"message": "400"}), query: "[50 TO *]"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_range_message_lower_bound_no_match {
            args: func_args![value: value!({"message": "400"}), query: "NOT [50 TO *]"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_range_message_lower_bound_no_match {
            args: func_args![value: value!({"message": "400"}), query: "-[50 TO *]"],
            want: Ok(true),
            tdef: type_def(),
        }

        range_message_lower_bound_string {
            args: func_args![value: value!({"message": "400"}), query: r#"["4" TO *]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_message_lower_bound_string {
            args: func_args![value: value!({"message": "400"}), query: r#"NOT ["4" TO *]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_message_lower_bound_string {
            args: func_args![value: value!({"message": "400"}), query: r#"-["4" TO *]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        range_message_lower_bound_string_no_match {
            args: func_args![value: value!({"message": "400"}), query: r#"["50" TO *]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        not_range_message_lower_bound_string_no_match {
            args: func_args![value: value!({"message": "400"}), query: r#"NOT ["50" TO *]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_range_message_lower_bound_string_no_match {
            args: func_args![value: value!({"message": "400"}), query: r#"-["50" TO *]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        range_message_upper_bound {
            args: func_args![value: value!({"message": "300"}), query: "[* TO 4]"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_message_upper_bound {
            args: func_args![value: value!({"message": "300"}), query: "NOT [* TO 4]"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_message_upper_bound {
            args: func_args![value: value!({"message": "300"}), query: "-[* TO 4]"],
            want: Ok(false),
            tdef: type_def(),
        }

        range_message_upper_bound_no_match {
            args: func_args![value: value!({"message": "50"}), query: "[* TO 400]"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_range_message_upper_bound_no_match {
            args: func_args![value: value!({"message": "50"}), query: "NOT [* TO 400]"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_range_message_upper_bound_no_match {
            args: func_args![value: value!({"message": "50"}), query: "-[* TO 400]"],
            want: Ok(true),
            tdef: type_def(),
        }

        range_message_upper_bound_string {
            args: func_args![value: value!({"message": "300"}), query: r#"[* TO "4"]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_message_upper_bound_string {
            args: func_args![value: value!({"message": "300"}), query: r#"NOT [* TO "4"]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_message_upper_bound_string {
            args: func_args![value: value!({"message": "300"}), query: r#"-[* TO "4"]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        range_message_upper_bound_string_no_match {
            args: func_args![value: value!({"message": "50"}), query: r#"[* TO "400"]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        not_range_message_upper_bound_string_no_match {
            args: func_args![value: value!({"message": "50"}), query: r#"NOT [* TO "400"]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_range_message_upper_bound_string_no_match {
            args: func_args![value: value!({"message": "50"}), query: r#"NOT [* TO "400"]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        range_message_between {
            args: func_args![value: value!({"message": 500}), query: "[1 TO 6]"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_message_between {
            args: func_args![value: value!({"message": 500}), query: "NOT [1 TO 6]"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_message_between {
            args: func_args![value: value!({"message": 500}), query: "-[1 TO 6]"],
            want: Ok(false),
            tdef: type_def(),
        }

        range_message_between_no_match {
            args: func_args![value: value!({"message": 70}), query: "[1 TO 6]"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_range_message_between_no_match {
            args: func_args![value: value!({"message": 70}), query: "NOT [1 TO 6]"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_range_message_between_no_match {
            args: func_args![value: value!({"message": 70}), query: "-[1 TO 6]"],
            want: Ok(true),
            tdef: type_def(),
        }

        range_message_between_string {
            args: func_args![value: value!({"message": "500"}), query: r#"["1" TO "6"]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_message_between_string {
            args: func_args![value: value!({"message": "500"}), query: r#"NOT ["1" TO "6"]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_message_between_string {
            args: func_args![value: value!({"message": "500"}), query: r#"-["1" TO "6"]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        range_message_between_no_match_string {
            args: func_args![value: value!({"message": "70"}), query: r#"["1" TO "6"]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        not_range_message_between_no_match_string {
            args: func_args![value: value!({"message": "70"}), query: r#"NOT ["1" TO "6"]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_range_message_between_no_match_string {
            args: func_args![value: value!({"message": "70"}), query: r#"-["1" TO "6"]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        range_tag_key {
            args: func_args![value: value!({"tags": ["a"]}), query: "a:[* TO *]"],
            want: Ok(true),
            tdef: type_def(),
        }

        range_tag_key_no_match {
            args: func_args![value: value!({"tags": ["b"]}), query: "a:[* TO *]"],
            want: Ok(false),
            tdef: type_def(),
        }

        range_tag_unbounded {
            args: func_args![value: value!({"tags": ["a:1"]}), query: "a:[* TO *]"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_tag_unbounded {
            args: func_args![value: value!({"tags": ["a:1"]}), query: "NOT a:[* TO *]"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_tag_unbounded {
            args: func_args![value: value!({"tags": ["a:1"]}), query: "-a:[* TO *]"],
            want: Ok(false),
            tdef: type_def(),
        }

        range_tag_lower_bound {
            args: func_args![value: value!({"tags": ["a:400"]}), query: "a:[4 TO *]"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_tag_lower_bound {
            args: func_args![value: value!({"tags": ["a:400"]}), query: "NOT a:[4 TO *]"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_tag_lower_bound {
            args: func_args![value: value!({"tags": ["a:400"]}), query: "-a:[4 TO *]"],
            want: Ok(false),
            tdef: type_def(),
        }

        range_tag_lower_bound_no_match {
            args: func_args![value: value!({"tags": ["a:400"]}), query: "a:[50 TO *]"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_range_tag_lower_bound_no_match {
            args: func_args![value: value!({"tags": ["a:400"]}), query: "NOT a:[50 TO *]"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_range_tag_lower_bound_no_match {
            args: func_args![value: value!({"tags": ["a:400"]}), query: "-a:[50 TO *]"],
            want: Ok(true),
            tdef: type_def(),
        }

        range_tag_lower_bound_string {
            args: func_args![value: value!({"tags": ["a:400"]}), query: r#"a:["4" TO *]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_tag_lower_bound_string {
            args: func_args![value: value!({"tags": ["a:400"]}), query: r#"NOT a:["4" TO *]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_tag_lower_bound_string {
            args: func_args![value: value!({"tags": ["a:400"]}), query: r#"-a:["4" TO *]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        range_tag_lower_bound_string_no_match {
            args: func_args![value: value!({"tags": ["a:400"]}), query: r#"a:["50" TO *]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        not_range_tag_lower_bound_string_no_match {
            args: func_args![value: value!({"tags": ["a:400"]}), query: r#"NOT a:["50" TO *]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_range_tag_lower_bound_string_no_match {
            args: func_args![value: value!({"tags": ["a:400"]}), query: r#"-a:["50" TO *]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        range_tag_upper_bound {
            args: func_args![value: value!({"tags": ["a:300"]}), query: "a:[* TO 4]"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_tag_upper_bound {
            args: func_args![value: value!({"tags": ["a:300"]}), query: "NOT a:[* TO 4]"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_tag_upper_bound {
            args: func_args![value: value!({"tags": ["a:300"]}), query: "-a:[* TO 4]"],
            want: Ok(false),
            tdef: type_def(),
        }

        range_tag_upper_bound_no_match {
            args: func_args![value: value!({"tags": ["a:50"]}), query: "a:[* TO 400]"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_range_tag_upper_bound_no_match {
            args: func_args![value: value!({"tags": ["a:50"]}), query: "NOT a:[* TO 400]"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_range_tag_upper_bound_no_match {
            args: func_args![value: value!({"tags": ["a:50"]}), query: "-a:[* TO 400]"],
            want: Ok(true),
            tdef: type_def(),
        }

        range_tag_upper_bound_string {
            args: func_args![value: value!({"tags": ["a:300"]}), query: r#"a:[* TO "4"]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_tag_upper_bound_string {
            args: func_args![value: value!({"tags": ["a:300"]}), query: r#"NOT a:[* TO "4"]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_tag_upper_bound_string {
            args: func_args![value: value!({"tags": ["a:300"]}), query: r#"-a:[* TO "4"]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        range_tag_upper_bound_string_no_match {
            args: func_args![value: value!({"tags": ["a:50"]}), query: r#"a:[* TO "400"]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        not_range_tag_upper_bound_string_no_match {
            args: func_args![value: value!({"tags": ["a:50"]}), query: r#"NOT a:[* TO "400"]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_range_tag_upper_bound_string_no_match {
            args: func_args![value: value!({"tags": ["a:50"]}), query: r#"-a:[* TO "400"]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        range_tag_between {
            args: func_args![value: value!({"tags": ["a:500"]}), query: "a:[1 TO 6]"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_tag_between {
            args: func_args![value: value!({"tags": ["a:500"]}), query: "NOT a:[1 TO 6]"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_tag_between {
            args: func_args![value: value!({"tags": ["a:500"]}), query: "-a:[1 TO 6]"],
            want: Ok(false),
            tdef: type_def(),
        }

        range_tag_between_no_match {
            args: func_args![value: value!({"tags": ["a:70"]}), query: "a:[1 TO 6]"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_range_tag_between_no_match {
            args: func_args![value: value!({"tags": ["a:70"]}), query: "NOT a:[1 TO 6]"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_range_tag_between_no_match {
            args: func_args![value: value!({"tags": ["a:70"]}), query: "-a:[1 TO 6]"],
            want: Ok(true),
            tdef: type_def(),
        }

        range_tag_between_string {
            args: func_args![value: value!({"tags": ["a:500"]}), query: r#"a:["1" TO "6"]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_tag_between_string {
            args: func_args![value: value!({"tags": ["a:500"]}), query: r#"NOT a:["1" TO "6"]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_tag_between_string {
            args: func_args![value: value!({"tags": ["a:500"]}), query: r#"-a:["1" TO "6"]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        range_tag_between_no_match_string {
            args: func_args![value: value!({"tags": ["a:70"]}), query: r#"a:["1" TO "6"]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        not_range_tag_between_no_match_string {
            args: func_args![value: value!({"tags": ["a:70"]}), query: r#"NOT a:["1" TO "6"]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_range_tag_between_no_match_string {
            args: func_args![value: value!({"tags": ["a:70"]}), query: r#"-a:["1" TO "6"]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        range_facet_unbounded {
            args: func_args![value: value!({"custom": {"a": 1}}), query: "@a:[* TO *]"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_facet_unbounded {
            args: func_args![value: value!({"custom": {"a": 1}}), query: "NOT @a:[* TO *]"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_facet_unbounded {
            args: func_args![value: value!({"custom": {"a": 1}}), query: "-@a:[* TO *]"],
            want: Ok(false),
            tdef: type_def(),
        }

        range_facet_lower_bound {
            args: func_args![value: value!({"custom": {"a": 5}}), query: "@a:[4 TO *]"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_facet_lower_bound {
            args: func_args![value: value!({"custom": {"a": 5}}), query: "NOT @a:[4 TO *]"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_facet_lower_bound {
            args: func_args![value: value!({"custom": {"a": 5}}), query: "-@a:[4 TO *]"],
            want: Ok(false),
            tdef: type_def(),
        }

        range_facet_lower_bound_no_match {
            args: func_args![value: value!({"custom": {"a": 5}}), query: "@a:[50 TO *]"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_range_facet_lower_bound_no_match {
            args: func_args![value: value!({"custom": {"a": 5}}), query: "NOT @a:[50 TO *]"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_range_facet_lower_bound_no_match {
            args: func_args![value: value!({"custom": {"a": 5}}), query: "-@a:[50 TO *]"],
            want: Ok(true),
            tdef: type_def(),
        }

        range_facet_lower_bound_string {
            args: func_args![value: value!({"custom": {"a": "5"}}), query: r#"@a:["4" TO *]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_facet_lower_bound_string {
            args: func_args![value: value!({"custom": {"a": "5"}}), query: r#"NOT @a:["4" TO *]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_facet_lower_bound_string {
            args: func_args![value: value!({"custom": {"a": "5"}}), query: r#"-@a:["4" TO *]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        range_facet_lower_bound_string_no_match {
            args: func_args![value: value!({"custom": {"a": "400"}}), query: r#"@a:["50" TO *]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        not_range_facet_lower_bound_string_no_match {
            args: func_args![value: value!({"custom": {"a": "400"}}), query: r#"NOT @a:["50" TO *]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_range_facet_lower_bound_string_no_match {
            args: func_args![value: value!({"custom": {"a": "400"}}), query: r#"-@a:["50" TO *]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        range_facet_upper_bound {
            args: func_args![value: value!({"custom": {"a": 1}}), query: "@a:[* TO 4]"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_facet_upper_bound {
            args: func_args![value: value!({"custom": {"a": 1}}), query: "NOT @a:[* TO 4]"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_facet_upper_bound {
            args: func_args![value: value!({"custom": {"a": 1}}), query: "-@a:[* TO 4]"],
            want: Ok(false),
            tdef: type_def(),
        }

        range_facet_upper_bound_no_match {
            args: func_args![value: value!({"custom": {"a": 500}}), query: "@a:[* TO 400]"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_range_facet_upper_bound_no_match {
            args: func_args![value: value!({"custom": {"a": 500}}), query: "NOT @a:[* TO 400]"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_range_facet_upper_bound_no_match {
            args: func_args![value: value!({"custom": {"a": 500}}), query: "-@a:[* TO 400]"],
            want: Ok(true),
            tdef: type_def(),
        }

        range_facet_upper_bound_string {
            args: func_args![value: value!({"custom": {"a": "3"}}), query: r#"@a:[* TO "4"]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_facet_upper_bound_string {
            args: func_args![value: value!({"custom": {"a": "3"}}), query: r#"NOT @a:[* TO "4"]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_facet_upper_bound_string {
            args: func_args![value: value!({"custom": {"a": "3"}}), query: r#"-@a:[* TO "4"]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        range_facet_upper_bound_string_no_match {
            args: func_args![value: value!({"custom": {"a": "5"}}), query: r#"@a:[* TO "400"]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        not_range_facet_upper_bound_string_no_match {
            args: func_args![value: value!({"custom": {"a": "5"}}), query: r#"NOT @a:[* TO "400"]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_range_facet_upper_bound_string_no_match {
            args: func_args![value: value!({"custom": {"a": "5"}}), query: r#"-@a:[* TO "400"]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        range_facet_between {
            args: func_args![value: value!({"custom": {"a": 5}}), query: "@a:[1 TO 6]"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_facet_between {
            args: func_args![value: value!({"custom": {"a": 5}}), query: "NOT @a:[1 TO 6]"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_facet_between {
            args: func_args![value: value!({"custom": {"a": 5}}), query: "-@a:[1 TO 6]"],
            want: Ok(false),
            tdef: type_def(),
        }

        range_facet_between_no_match {
            args: func_args![value: value!({"custom": {"a": 200}}), query: "@a:[1 TO 6]"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_range_facet_between_no_match {
            args: func_args![value: value!({"custom": {"a": 200}}), query: "NOT @a:[1 TO 6]"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_range_facet_between_no_match {
            args: func_args![value: value!({"custom": {"a": 200}}), query: "-@a:[1 TO 6]"],
            want: Ok(true),
            tdef: type_def(),
        }

        range_facet_between_string {
            args: func_args![value: value!({"custom": {"a": "500"}}), query: r#"@a:["1" TO "6"]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        not_range_facet_between_string {
            args: func_args![value: value!({"custom": {"a": "500"}}), query: r#"NOT @a:["1" TO "6"]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_range_facet_between_string {
            args: func_args![value: value!({"custom": {"a": "500"}}), query: r#"-@a:["1" TO "6"]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        range_facet_between_no_match_string {
            args: func_args![value: value!({"custom": {"a": "7"}}), query: r#"@a:["1" TO "60"]"#],
            want: Ok(false),
            tdef: type_def(),
        }

        not_range_facet_between_no_match_string {
            args: func_args![value: value!({"custom": {"a": "7"}}), query: r#"NOT @a:["1" TO "60"]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_range_facet_between_no_match_string {
            args: func_args![value: value!({"custom": {"a": "7"}}), query: r#"-@a:["1" TO "60"]"#],
            want: Ok(true),
            tdef: type_def(),
        }

        exclusive_range_message {
            args: func_args![value: value!({"message": "100"}), query: "{1 TO 2}"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_exclusive_range_message {
            args: func_args![value: value!({"message": "100"}), query: "NOT {1 TO 2}"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_exclusive_range_message {
            args: func_args![value: value!({"message": "100"}), query: "-{1 TO 2}"],
            want: Ok(false),
            tdef: type_def(),
        }

        exclusive_range_message_no_match {
            args: func_args![value: value!({"message": "1"}), query: "{1 TO 2}"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_exclusive_range_message_no_match {
            args: func_args![value: value!({"message": "1"}), query: "NOT {1 TO 2}"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_exclusive_range_message_no_match {
            args: func_args![value: value!({"message": "1"}), query: "-{1 TO 2}"],
            want: Ok(true),
            tdef: type_def(),
        }

        exclusive_range_message_lower {
            args: func_args![value: value!({"message": "200"}), query: "{1 TO *}"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_exclusive_range_message_lower {
            args: func_args![value: value!({"message": "200"}), query: "NOT {1 TO *}"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_exclusive_range_message_lower {
            args: func_args![value: value!({"message": "200"}), query: "-{1 TO *}"],
            want: Ok(false),
            tdef: type_def(),
        }

        exclusive_range_message_lower_no_match {
            args: func_args![value: value!({"message": "1"}), query: "{1 TO *}"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_exclusive_range_message_lower_no_match {
            args: func_args![value: value!({"message": "1"}), query: "NOT {1 TO *}"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_exclusive_range_message_lower_no_match {
            args: func_args![value: value!({"message": "1"}), query: "-{1 TO *}"],
            want: Ok(true),
            tdef: type_def(),
        }

        exclusive_range_message_upper {
            args: func_args![value: value!({"message": "200"}), query: "{* TO 3}"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_exclusive_range_message_upper {
            args: func_args![value: value!({"message": "200"}), query: "NOT {* TO 3}"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_exclusive_range_message_upper {
            args: func_args![value: value!({"message": "200"}), query: "-{* TO 3}"],
            want: Ok(false),
            tdef: type_def(),
        }

        exclusive_range_message_upper_no_match {
            args: func_args![value: value!({"message": "3"}), query: "{* TO 3}"],
            want: Ok(false),
            tdef: type_def(),
        }

        not_exclusive_range_message_upper_no_match {
            args: func_args![value: value!({"message": "3"}), query: "NOT {* TO 3}"],
            want: Ok(true),
            tdef: type_def(),
        }

        negate_exclusive_range_message_upper_no_match {
            args: func_args![value: value!({"message": "3"}), query: "-{* TO 3}"],
            want: Ok(true),
            tdef: type_def(),
        }

        message_and {
            args: func_args![value: value!({"message": "this contains that"}), query: "this AND that"],
            want: Ok(true),
            tdef: type_def(),
        }

        message_and_not {
            args: func_args![value: value!({"message": "this contains that"}), query: "this AND NOT that"],
            want: Ok(false),
            tdef: type_def(),
        }

        message_or {
            args: func_args![value: value!({"message": "only contains that"}), query: "this OR that"],
            want: Ok(true),
            tdef: type_def(),
        }

        message_or_not {
            args: func_args![value: value!({"message": "only contains that"}), query: "this OR NOT that"],
            want: Ok(false),
            tdef: type_def(),
        }

        message_and_or {
            args: func_args![value: value!({"message": "this contains that"}), query: "this AND (that OR the_other)"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_message_and_or {
            args: func_args![value: value!({"message": "this contains that"}), query: "this AND NOT (that OR the_other)"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_message_and_or {
            args: func_args![value: value!({"message": "this contains that"}), query: "this AND -(that OR the_other)"],
            want: Ok(false),
            tdef: type_def(),
        }

        message_and_or_2 {
            args: func_args![value: value!({"message": "this contains the_other"}), query: "this AND (that OR the_other)"],
            want: Ok(true),
            tdef: type_def(),
        }

        not_message_and_or_2 {
            args: func_args![value: value!({"message": "this contains the_other"}), query: "this AND NOT (that OR the_other)"],
            want: Ok(false),
            tdef: type_def(),
        }

        negate_message_and_or_2 {
            args: func_args![value: value!({"message": "this contains the_other"}), query: "this AND -(that OR the_other)"],
            want: Ok(false),
            tdef: type_def(),
        }

        message_or_and {
            args: func_args![value: value!({"message": "just this"}), query: "this OR (that AND the_other)"],
            want: Ok(true),
            tdef: type_def(),
        }

        message_or_and_no_match {
            args: func_args![value: value!({"message": "that and nothing else"}), query: "this OR (that AND the_other)"],
            want: Ok(false),
            tdef: type_def(),
        }

        message_or_and_2 {
            args: func_args![value: value!({"message": "that plus the_other"}), query: "this OR (that AND the_other)"],
            want: Ok(true),
            tdef: type_def(),
        }

        message_or_and_2_no_match {
            args: func_args![value: value!({"message": "nothing plus the_other"}), query: "this OR (that AND the_other)"],
            want: Ok(false),
            tdef: type_def(),
        }

        kitchen_sink {
            args: func_args![value: value!({"host": "this"}), query: "host:this OR ((@b:test* AND c:that) AND d:the_other @e:[1 TO 5])"],
            want: Ok(true),
            tdef: type_def(),
        }

        kitchen_sink_2 {
            args: func_args![value: value!({"tags": ["c:that", "d:the_other"], "custom": {"b": "testing", "e": 3}}), query: "host:this OR ((@b:test* AND c:that) AND d:the_other @e:[1 TO 5])"],
            want: Ok(true),
            tdef: type_def(),
        }
    ];
}
