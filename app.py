import re
import json


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


def to_snake_case(name: str) -> str:
    """
    Convert a string (e.g. CamelCase or mixedCase) to snake_case.

    Arg(s):
        name (str): The string to convert.

    Returns:
        str: The converted string.
    """
    # Insert an underscore before each capital letter (that isn't at start) and lowercase everything.
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()


def add_alias_to_record(schema: dict, prepend_subject_name: str) -> None:
    """
    If the schema is a record and has a 'name', add an aliases array
    containing the snake_case version of that name.

    Arg(s):
        schema (dict): The schema to update.
        prepend_subject_name (str): The prepended string that needs to
                                    be removed from the record names.
    """
    # Only do this if 'name' exists in the record definition
    if "name" in schema:
        snake_case_name = to_snake_case(schema["name"].removeprefix(prepend_subject_name))
        # If 'aliases' already present, just append (or ignore if you want strict one-value aliases).
        if "aliases" in schema:
            schema["aliases"].append(snake_case_name)
        else:
            schema["aliases"] = [snake_case_name]


def add_alias_to_field(field: dict) -> None:
    """
    Add an aliases array to the field containing the snake_case version of that field name.

    Arg(s):
        field (dict): The field to update.
    """
    snake_case_name = to_snake_case(field["name"])
    if "aliases" in field:
        field["aliases"].append(snake_case_name)
    else:
        field["aliases"] = [snake_case_name]


def traverse_schema(schema: dict, prepend_subject_name: str) -> None:
    """
    Recursively traverse the Avro schema, adding snake_case aliases
    to each record and each field.

    Arg(s):
        schema (dict): The schema to update.
        prepend_subject_name (str): The prepended string that needs to
                                    be removed from the record names.
    """
    schema_type = schema.get("type")

    if schema_type == "record":
        add_alias_to_record(schema, prepend_subject_name)

        # Iterate through all the fields.
        fields = schema.get("fields", [])
        for field in fields:
            add_alias_to_field(field)

            # The field type can be:
            #   - string (simple type)
            #   - dict (nested record, enum, fixed, etc.)
            #   - list (union of multiple types)
            #   - or a complex combination
            field_type = field["type"]
            _traverse_type(field_type, prepend_subject_name)
    elif schema_type == "array":
        items = schema.get("items")
        _traverse_type(items, prepend_subject_name)


def _traverse_type(type_obj, prepend_subject_name: str) -> None:
    """
    Helper to handle different Avro `type` variants (e.g. dict, list, string).

    Arg(s):
        type_obj: The type object to traverse.
        prepend_subject_name (str): The prepended string that needs to
                                    be removed from the record names.
    """
    if isinstance(type_obj, dict):
        # We have a nested schema.
        traverse_schema(type_obj, prepend_subject_name)
    elif isinstance(type_obj, list):
        # We have a union; each element can be a simple type or a dict.
        for t in type_obj:
            if isinstance(t, dict):
                traverse_schema(t, prepend_subject_name)
    # If it's a simple primitive, then we do nothing.


def add_aliases_to_avro_schema(schema: dict, prepend_subject_name: str) -> dict:
    """
    Public-facing function to update an Avro schema in-place
    (or you can return a copy if desired) to add snake_case aliases.

    Arg(s):
        schema (dict): The schema to update.
        prepend_subject_name (str): The prepended string that needs to
                                    be removed from the record names.

    Returns:
        dict: The updated schema.
    """
    traverse_schema(schema, prepend_subject_name)
    return schema


def load_schema(file_name: str) -> dict:
    """
    This method reads in a file.
 
    Arg(s):
        file_name (str):   The name of the file.
 
    Returns:
        The file content.
    """
    with open(file_name, 'r') as file:
        schema = json.load(file)
    return schema


def save_schema(file_name: str, updated_schema: dict) -> None:
    """
    This method reads in a file.
 
    Arg(s):
        file_name (str):   The name of the file.
        updated_schema (dict): The updated schema.
    """
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(updated_schema, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    avsc_file = load_schema("<ORIGINAL_AVRO_SCHEMA>")
    updated_schema = add_aliases_to_avro_schema(avsc_file, "<PREENDED_VALUE>")
    save_schema("<NEW_AVRO_SCHEMA>", updated_schema)
