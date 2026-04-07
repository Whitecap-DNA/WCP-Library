import secrets
import string


class MissingCredentialsError(KeyError):
    pass


def generate_password(length: int=12, use_nums: bool=True, use_special: bool=True, special_chars_override: str | None=None, force_num: bool=True, force_spec: bool=True, max_attempts: int=1000) -> str:
    """
    Function to generate a random password

    :param length: Length of the password
    :param use_nums: Allows the use of numbers
    :param use_special: Allows the use of special characters
    :param special_chars_override: List of special characters to use
    :param force_num: Requires the password to contain at least one number
    :param force_spec: Requires the password to contain at least one special character
    :param max_attempts: Maximum number of attempts to generate a valid password
    :return: Password
    :raises ValueError: If unable to generate a valid password within max_attempts
    """

    if length < 1:
        raise ValueError("Password length must be at least 1")

    if (force_num and not use_nums) or (force_spec and not use_special):
        raise ValueError("Cannot force character types that are not allowed")

    letters = string.ascii_letters
    digits = string.digits
    special_chars = special_chars_override if special_chars_override else string.punctuation

    alphabet = letters
    if use_nums:
        alphabet += digits
    if use_special:
        alphabet += special_chars

    for attempt in range(max_attempts):
        pwd = ''.join(secrets.choice(alphabet) for _ in range(length))

        # Check constraints
        valid = True

        # First character cannot be a digit
        if pwd[0].isdigit():
            valid = False
            continue

        # Must contain at least one number if forced
        if use_nums and force_num and not any(char.isdigit() for char in pwd):
            valid = False
            continue

        # Must contain at least one special character if forced
        if use_special and force_spec and not any(char in special_chars for char in pwd):
            valid = False
            continue

        if valid:
            return pwd

    raise ValueError(f"Unable to generate a valid password after {max_attempts} attempts")