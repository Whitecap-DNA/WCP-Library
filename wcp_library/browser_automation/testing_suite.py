"""
PyTest module to test out all the browser and interactions functions
"""
# pylint: disable=redefined-outer-name

from datetime import datetime
import pytest
from wcp_library.browser_automation.browser import Browser, Firefox, Edge, Chrome

URL = "https://scriptng.com/practise-site/selenium-ui-elements/"
FOLDER_PATH = "C:/Users/benjamin.clabaux/Documents/Testing Folder"

# Define browser classes to test
BROWSER_CLASSES = [Firefox, Edge, Chrome]

# Define option sets to test
BROWSER_OPTIONS = [
    {
        "explicit_wait": 10,
        "implicit_wait": 30,
        "download_path": FOLDER_PATH,
    },
    {
        "explicit_wait": 30,
        "args": ["--headless", "--disable-gpu"],
    },
    {
        "page_load_strategy": "eager",
        "explicit_wait": 5,
    },
    {
        "unhandled_prompt_behavior": "dismiss",
        "args": ["--start-maximized"],
        "timeouts": {"implicit": 5000},
    },
]


@pytest.fixture(
    params=[
        (browser, options) for browser in BROWSER_CLASSES for options in BROWSER_OPTIONS
    ]
)
def setup_driver(request):
    """
    Pytest fixture to initialize and yield a browser driver instance with specified parameters.

    This fixture sets up the browser using the provided browser class and options,
    navigates to the predefined URL, and yields the driver for use in tests.
    The driver is automatically closed after the test completes.

    Args:
        request: A pytest fixture parameter containing a tuple of (browser_class, options).

    Yields:
        An instance of the initialized browser driver.
    """
    browser_class, options = request.param
    print(f"Setting up {browser_class.__name__} with options: {options}")
    with Browser(browser_class, options) as driver:
        driver.go_to(URL)
        yield driver


############################################################################################
#                                                                                          #
#                                                                                          #
#                              Testing browser navigation                                  #
#                                                                                          #
#                                                                                          #
############################################################################################


def test_go_to(setup_driver):
    """
    Test navigation to a specified URL using the setup driver.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    setup_driver.go_to(URL)


def test_get_url(setup_driver):
    """
    Test retrieval of the current URL from the browser.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    current_url = setup_driver.get_url()
    print(f"Returned URL: {current_url}")


def test_get_title(setup_driver):
    """
    Test retrieval of the current page title from the browser.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    title = setup_driver.get_title()
    print(f"Returned Title: {title}")


def test_take_screenshot(setup_driver):
    """
    Test taking a screenshot of the current browser window and saving it to a file.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    screenshot_file_location = (
        f"{FOLDER_PATH}/{datetime.now().strftime('%Y-%m-%d_%H-%M')}.png"
    )
    setup_driver.take_screenshot(screenshot_file_location)
    print(f"Screenshot saved successfully at {screenshot_file_location}")


def test_if_web_page_contains(setup_driver):
    """
    Test to check if the web page contains an element.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    exists = setup_driver.if_web_page_contains("footer", locator="tag_name")
    print(f"Element exists: {exists}")


def test_if_web_page_contains_we(setup_driver):
    """
    Test to check if the web page contains an element using a web element.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Checking if web page contains element using web element")
    driver = setup_driver
    element = driver.get_element("footer", locator="tag_name")
    exists = driver.if_web_page_contains(element)
    print(f"Element exists: {exists}")


def test_wait_for_element(setup_driver):
    """
    Test to wait for an element.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Waiting for element by ID")
    driver = setup_driver
    el = driver.wait_for_element(
        "visibility-status", locator="id", expected_condition="visible", wait_time=15
    )
    print(f"Returned Element: {el}")


def test_wait_for_element_we(setup_driver):
    """
    Test to wait for an element using a web element.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Waiting for element using web element")
    driver = setup_driver
    element = driver.get_element(
        "visibility-status", locator="id", expected_condition="visible", wait_time=15
    )
    el = driver.wait_for_element_we(element, wait_time=10)
    print(f"Returned Element: {el}")


############################################################################################
#                                                                                          #
#                                                                                          #
#                     Testing element search with multiple permutations                    #
#                                                                                          #
#                                                                                          #
############################################################################################


def test_get_element_with_id(setup_driver):
    """
    Test to get a web element by its id.

        Args: setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting element by ID")
    driver = setup_driver
    element = driver.get_element("elementor-tab-title-9721", locator="id")
    print(f"Returned Element: {element}")


def test_get_element_with_class_name(setup_driver):
    """
    Test to get a web element by its class name.

        Args: setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting element by Class Name")
    driver = setup_driver
    element = driver.get_element("elementor-tab-desktop-title", locator="class_name")
    print(f"Returned Element: {element}")


def test_get_element_with_tag_name(setup_driver):
    """
    Test to get a web element by its tag name.

        Args: setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting element by Tag Name")
    driver = setup_driver
    element = driver.get_element("h5", locator="tag_name")
    print(f"Returned Element: {element}")


def test_get_element_with_xpath(setup_driver):
    """
    Test to get a web element by its xpath.

        Args: setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting element by XPath")
    driver = setup_driver
    element = driver.get_element(
        "/html/body/div/section[4]/div/div/div/div[1]/div/h5[1]", locator="xpath"
    )
    print(f"Returned Element: {element}")


def test_get_element_with_link_text(setup_driver):
    """
    Test to get a web element by its link text.

        Args: setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting element by Link Text")
    driver = setup_driver
    element = driver.get_element("Home", locator="link_text")
    print(f"Returned Element: {element}")


def test_get_element_with_partial_link_text(setup_driver):
    """
    Test to get a web element by its partial link text.

        Args: setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting element by Partial Link Text")
    driver = setup_driver
    element = driver.get_element("Practise", locator="partial_link_text")
    print(f"Returned Element: {element}")


def test_get_element_with_css_selector(setup_driver):
    """
    Test to get a web element by its css selector.

        Args: setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting element by CSS Selector")
    driver = setup_driver
    element = driver.get_element("#elementor-tab-title-9721", locator="css")
    print(f"Returned Element: {element}")


def test_get_element(setup_driver):
    """
    Test to get a web element by the default locator.

        Args: setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting element by CSS Selector")
    driver = setup_driver
    element = driver.get_element("#elementor-tab-title-9721")
    print(f"Returned Element: {element}")


def test_get_multiple_elements_with_class_name(setup_driver):
    """
    Test to get multiple web elements by their class name.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting multiple elements by class name")
    driver = setup_driver
    elements = driver.get_multiple_elements(
        element_value="elementor-element", locator="class_name"
    )
    print(f"Returned Elements: {elements}")


def test_get_multiple_elements_with_tag_name(setup_driver):
    """
    Test to get multiple web elements by their tag name.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting multiple elements by tag name")
    driver = setup_driver
    elements = driver.get_multiple_elements(element_value="div", locator="tag_name")
    print(f"Returned Elements: {elements}")


def test_get_multiple_elements_with_xpath(setup_driver):
    """
    Test to get multiple web elements by their xpath.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting multiple elements by xpath")
    driver = setup_driver
    elements = driver.get_multiple_elements(
        element_value="//div[@class='elementor-element']", locator="xpath"
    )
    print(f"Returned Elements: {elements}")


def test_get_multiple_elements_with_link_text(setup_driver):
    """
    Test to get multiple web elements by their link text.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting multiple elements by link text")
    driver = setup_driver
    elements = driver.get_multiple_elements(
        element_value="Practise Site – Elements", locator="link_text"
    )
    print(f"Returned Elements: {elements}")


def test_get_multiple_elements_with_partial_link_text(setup_driver):
    """
    Test to get multiple web elements by their partial link text.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting multiple elements by partial link text")
    driver = setup_driver
    elements = driver.get_multiple_elements(
        element_value="Practise Site", locator="partial_link_text"
    )
    print(f"Returned Elements: {elements}")


def test_get_multiple_elements_with_css(setup_driver):
    """
    Test to get multiple web elements by their css.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting multiple elements by css")
    driver = setup_driver
    elements = driver.get_multiple_elements(
        element_value=".elementor-element", locator="css"
    )
    print(f"Returned Elements: {elements}")


def test_get_multiple_elements(setup_driver):
    """
    Test to get multiple web elements by the default locator.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting multiple elements by default locator")
    driver = setup_driver
    elements = driver.get_multiple_elements(element_value=".elementor-element")
    print(f"Returned Elements: {elements}")


############################################################################################
#                                                                                          #
#                                                                                          #
#                                Testing ui interactions                                   #
#                                                                                          #
#                                                                                          #
############################################################################################


def test_press_button(setup_driver):
    """
    Test to press a button.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Pressing button by ID")
    driver = setup_driver
    driver.press_button("submit", locator="id")
    print("Button pressed successfully")


def test_press_button_we(setup_driver):
    """
    Test to press a button using a web element.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Pressing button using web element")
    driver = setup_driver
    element = driver.get_element("submit", locator="id")
    driver.press_button(element)
    print("Button pressed successfully")


def test_get_text(setup_driver):
    """
    Test to get text.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting text by tag name")
    driver = setup_driver
    text = driver.get_text("h2.elementor-heading-title.elementor-size-default")
    print(f"Returned Text: {text}")


def test_get_text_we(setup_driver):
    """
    Test to get text using a web element.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting text using web element")
    driver = setup_driver
    element = driver.get_element("h2.elementor-heading-title.elementor-size-default")
    text = driver.get_text(element)
    print(f"Returned Text: {text}")


def test_get_value(setup_driver):
    """
    Test to get value.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting value by ID")
    driver = setup_driver
    value = driver.get_value("form-field-name", locator="id")
    print(f"Returned Value: {value}")


def test_get_value_we(setup_driver):
    """
    Test to get value using a web element.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting value using web element")
    driver = setup_driver
    element = driver.get_element("form-field-name", locator="id")
    value = driver.get_value(element)
    print(f"Returned Value: {value}")


# TODO
def test_get_table(setup_driver):
    """
    Test to get table data.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting table by ID")
    driver = setup_driver
    driver.go_to_url("https://www.w3schools.com/html/html_tables.asp")
    df = driver.get_table("customer", locator="id")
    print(f"Returned Table DataFrame: {df}")


# TODO
def test_get_table_we(setup_driver):
    """
    Test to get table data using a web element.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Getting table using web element")
    driver = setup_driver
    driver.go_to_url("https://www.w3schools.com/html/html_tables.asp")
    element = driver.get_element("customer", locator="id")
    print(f"Element: {element}")
    df = driver.get_table(element)
    print(f"Returned Table DataFrame: {df}")


def test_enter_text(setup_driver):
    """
    Test to populate a text field.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Populating text field by ID")
    driver = setup_driver
    driver.enter_text("Test input", "form-field-address", locator="id")
    print("Text field populated successfully")


def test_enter_text_we(setup_driver):
    """
    Test to populate a text field using a web element.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Populating text field using web element")
    driver = setup_driver
    element = driver.get_element("form-field-address", locator="id")
    driver.enter_text("Test input", element)
    print("Text field populated successfully")


def test_set_checkbox_state(setup_driver):
    """
    Test to set the state of a checkbox.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Setting checkbox state by ID")
    driver = setup_driver
    driver.press_button("elementor-tab-title-9722", locator="id")

    driver.set_checkbox_state(True, "form-field-language-0", locator="id")
    driver.set_checkbox_state(False, "form-field-language-3", locator="id")
    print("Checkbox state set successfully")


def test_set_checkbox_state_we(setup_driver):
    """
    Test to set the state of a checkbox using a web element.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Setting checkbox state using web element")
    driver = setup_driver
    driver.press_button("elementor-tab-title-9722", locator="id")

    element = driver.get_element("form-field-language-0", locator="id")
    driver.set_checkbox_state(False, element)
    element = driver.get_element("form-field-language-3", locator="id")
    driver.set_checkbox_state(True, element)
    print("Checkbox state set successfully")


# TODO
def test_set_select_option(setup_driver):
    """
    Test to set a select option.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Setting select option by ID")
    driver = setup_driver
    driver.go_to_url("https://www.w3schools.com/tags/tryit.asp?filename=tryhtml_select")

    driver.set_select_option("opel", "cars", locator="id")
    print("Select option set successfully")


# TODO
def test_set_select_option_we(setup_driver):
    """
    Test to set a select option using a web element.

    Args:
        setup_driver (WebDriver): The initialized WebDriver instance.
    """
    print("Setting select option using web element")
    driver = setup_driver
    driver.go_to_url("https://www.w3schools.com/tags/tryit.asp?filename=tryhtml_select")

    element = driver.get_element("cars", locator="id")
    driver.set_select_option("ca", element)
    print("Select option set successfully")
