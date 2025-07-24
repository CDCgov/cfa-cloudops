"""
Functions for handling configuration
files and environment variables.
"""

import os


def try_get_val_from_dict(
    key: str,
    config_dict: dict,
    value_name: str = None,
) -> tuple[str, str]:
    """Attempt to get a configuration value from a configuration dictionary.

    Return None along with an informative failure message if this is not possible.

    Args:
        key: Key under which to look up the value in the ``config_dict``.
        config_dict: Dictionary of configuration keys and values in which to look up the value.
        value_name: Descriptive name for the configuration value, to allow more readable
            failure messages when the ``key`` cannot be found in ``config_dict``.
            If None, use the value of ``key``.

    Returns:
        tuple[str, str]: A tuple where the first entry is the value if it was successfully
            retrieved and otherwise None. The second entry is None on success and otherwise
            a description of the failure, as a string.

    Example:
        >>> config = {"database_url": "localhost:5432", "debug": "true"}
        >>> value, error = try_get_val_from_dict("database_url", config)
        >>> print(value)  # "localhost:5432"
        >>> print(error)  # None

        >>> value, error = try_get_val_from_dict("missing_key", config)
        >>> print(value)  # None
        >>> print(error)  # "Could not find a configuration value..."
    """
    if value_name is None:
        value_name = key

    result = config_dict.get(key, None)

    if result is None:
        message = (
            "Could not find a configuration value "
            f"for '{value_name}' under the "
            f"key '{key}' in the provided "
            "configuration dictionary."
        )
    else:
        message = None

    return result, message


def try_get_val_from_env(
    env_variable_name: str, value_name: str = None
) -> tuple[str, str]:
    """Attempt to get a configuration value from local environment variables.

    Return None along with an informative failure message if this is not possible.

    Args:
        env_variable_name: Name of the environment variable to attempt to retrieve.
        value_name: Descriptive name for the value, for more readable failure messages
            when the ``env_variable_name`` cannot be located. If None, use the value
            of ``env_variable_name``.

    Returns:
        tuple[str, str]: A tuple where the first entry is the value of the variable
            if it was successfully retrieved and otherwise None. The second entry is
            None on success and otherwise a description of the failure, as a string.

    Example:
        >>> import os
        >>> os.environ["TEST_VAR"] = "test_value"
        >>> value, error = try_get_val_from_env("TEST_VAR")
        >>> print(value)  # "test_value"
        >>> print(error)  # None

        >>> value, error = try_get_val_from_env("MISSING_VAR")
        >>> print(value)  # None
        >>> print(error)  # "Could not find a valid configuration value..."
    """
    if value_name is None:
        value_name = env_variable_name

    result = os.environ.get(env_variable_name, None)

    if result is None:
        message = (
            "Could not find a valid configuration "
            f"value for '{value_name}' "
            "among available environment variables. "
            "Looked for an environment variable "
            f"named '{env_variable_name}', but no such "
            "environment variable was found."
        )
    else:
        message = None

    return result, message


def get_config_val(
    key: str,
    config_dict: dict = None,
    try_env: bool = True,
    env_variable_name: str = None,
    value_name: str = None,
) -> str:
    """Get a configuration variable from a configuration dictionary and/or from local environment variables.

    First consult a configuration dictionary, if one has been provided. Then consult
    environment variables if directed to do so. If no valid value can be found, raise a ValueError.

    Args:
        key: Key under which to look up the value in the ``config_dict``, if one is provided,
            and variable name to check in the environment variables, if a distinct
            ``env_variable_name`` is not provided.
        config_dict: Dictionary of configuration keys and values in which to look up the value.
            If None, only look in environment variables, provided ``try_env`` is True.
        try_env: Look for the value in the environment variables if it cannot be found in the
            ``config_dict``? If True, will look for an environment variable corresponding to
            ``key``, but with that string converted to uppercase (i.e. the output of ``key.upper()``),
            unless a custom name is provided via the ``env_variable_name`` argument.
        env_variable_name: Environmental variable name to check for the variable, if it cannot
            be found in the config. If None and ``try_env`` is True, use the value of ``key.upper()``.
        value_name: Descriptive name for the configuration value, to produce more informative
            error messages when a valid value cannot be found. If None, use the value of ``key``.

    Returns:
        str: The configuration value.

    Raises:
        ValueError: If the value cannot be found either in the configuration dictionary or in
            an environment variable, or if ``config_dict`` is None and ``try_env`` is False.

    Example:
        >>> import os
        >>> os.environ["DATABASE_URL"] = "localhost:5432"
        >>>
        >>> # Get from environment variable
        >>> value = get_config_val("database_url")  # Looks for DATABASE_URL
        >>> print(value)  # "localhost:5432"
        >>>
        >>> # Get from config dict with fallback to env
        >>> config = {"api_key": "secret123"} #pragma: allowlist secret
        >>> value = get_config_val("api_key", config_dict=config)
        >>> print(value)  # "secret123"
        >>>
        >>> # Custom environment variable name
        >>> value = get_config_val("db", env_variable_name="DATABASE_URL")
        >>> print(value)  # "localhost:5432"
    """

    if value_name is None:
        value_name = key
    if env_variable_name is None:
        env_variable_name = key.upper()

    dict_result, env_result = None, None

    if config_dict is None and not try_env:
        print(
            "Must either provide a configuration dictionary "
            "via the `config_dict` argument or allow for "
            "inspecting environment variables by setting "
            "`try_env` to `True`. Got no configuration "
            "dictionary, and `try_env` was set to `False`."
        )

    if config_dict is not None:
        dict_result, dict_msg = try_get_val_from_dict(
            key=key,
            config_dict=config_dict,
            value_name=value_name,
        )

    if try_env:
        env_result, env_msg = try_get_val_from_env(
            env_variable_name=env_variable_name, value_name=value_name
        )

    result = dict_result if dict_result is not None else env_result

    if result is None:
        if config_dict is not None:
            err_msg = dict_msg + (
                (" Also searched environment variables. " + env_msg)
                if try_env
                else ""
            )
        else:
            err_msg = env_msg
        print(err_msg)

    return result
