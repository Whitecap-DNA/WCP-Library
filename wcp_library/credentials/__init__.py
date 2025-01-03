import secrets
import string


class MissingCredentialsError(KeyError):
    pass


def generate_password(length: int=12, use_nums: bool=True, use_special: bool=True, special_chars_override: str=None, force_num: bool=True, force_spec: bool=True) -> str:
    """
    Function to generate a random password

    :param length:
    :param use_nums: Allows the use of numbers
    :param use_special: Allows the use of special characters
    :param special_chars_override: List of special characters to use
    :param force_num: Requires the password to contain at least one number
    :param force_spec: Requires the password to contain at least one special character
    :return: Password
    """

    letters = string.ascii_letters
    digits = string.digits
    if special_chars_override:
        special_chars = special_chars_override
    else:
        special_chars = string.punctuation

    alphabet = letters
    if use_nums:
        alphabet += digits
    if use_special:
        alphabet += special_chars

    pwd = ''
    for i in range(length):
        pwd += ''.join(secrets.choice(alphabet))

    if (use_nums and force_num) and (use_special and force_spec):
        while pwd[0].isdigit() or not any(char.isdigit() for char in pwd) or not any(char in pwd for char in special_chars):
            pwd = generate_password(length, use_nums, use_special, special_chars_override, force_num, force_spec)
    elif use_nums and force_num:
        while pwd[0].isdigit() or not any(char.isdigit() for char in pwd):
            pwd = generate_password(length, use_nums, use_special, special_chars_override, force_num, force_spec)
    elif use_special and force_spec:
        while not any(char in pwd for char in special_chars):
            pwd = generate_password(length, use_nums, use_special, special_chars_override, force_num, force_spec)

    return pwd