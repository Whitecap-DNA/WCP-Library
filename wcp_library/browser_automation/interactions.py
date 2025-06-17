"""
This module provides classes and methods for interacting with web elements using Selenium WebDriver.

The module contains the following classes:
- Interactions: A base class for common web interactions.
- UIInteractions: A subclass of Interactions for interacting with web elements using locators.
- WEInteractions: A subclass of Interactions for interacting with web elements directly.

Each class provides methods for performing various web interactions such as navigating to a URL,
taking screenshots, waiting for elements, clicking buttons, entering text, and more.

Example usage:
    from selenium import webdriver
    from interactions import UIInteractions

    driver = webdriver.Chrome()
    ui_interactions = UIInteractions(driver)
    ui_interactions.go_to("https://example.com")
    title = ui_interactions.get_title()
    print(title)
"""

import logging
from datetime import datetime
from io import StringIO
from typing import List, Optional

import pandas as pd
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

EXECUTION_ERROR_SCREENSHOT_FOLDER = "P:/Python/RPA/Execution Error Screenshots"


class Interactions:
    """Class for interacting with web elements using Selenium WebDriver.

    Attributes:
        driver: The Selenium WebDriver instance.
    """

    def __init__(self, driver):
        self.driver = driver
        logging.basicConfig(level=logging.INFO)

    def go_to(self, url: str):
        """Navigate to the specified URL.

        Args:
            url (str): The URL to navigate to.

        Raises:
            RuntimeError: If the WebDriver is not initialized.
        """
        if self.driver:
            self.driver.get(url)
        else:
            raise RuntimeError("WebDriver is not initialized.")

    def get_url(self) -> str:
        """Get the current URL of the page.

        Returns:
            str: The current URL.

        Raises:
            RuntimeError: If the WebDriver is not initialized.
        """
        if self.driver:
            return self.driver.current_url
        raise RuntimeError("WebDriver is not initialized.")

    def get_title(self) -> str:
        """Get the title of the current page.

        Returns:
            str: The title of the current page.

        Raises:
            RuntimeError: If the WebDriver is not initialized.
        """
        if self.driver:
            return self.driver.title
        raise RuntimeError("WebDriver is not initialized.")

    def take_screenshot(self, file_path: str):
        """Take a screenshot of the current page and save it to the specified file path.

        Args:
            file_path (str): The path where the screenshot will be saved.

        Raises:
            RuntimeError: If the WebDriver is not initialized.
        """
        if self.driver:
            self.driver.save_screenshot(file_path)
        else:
            raise RuntimeError("WebDriver is not initialized.")

    def _take_error_screenshot(self):
        """Take a screenshot of the current page and save it to the P drive."""
        self.take_screenshot(
            f"{EXECUTION_ERROR_SCREENSHOT_FOLDER}/Failure Screenshot - {datetime.now().strftime('%Y-%m-%d_%H-%M')}.png"
        )

    def _get_expected_condition(self, expected_condition: Optional[str]) -> EC:
        """Get the expected condition based on the provided string.

        Args:
            expected_condition (Optional[str]): The expected condition type.

        Returns:
            EC: The expected condition object.
        """
        match expected_condition:
            case "present":
                expected_condition = EC.presence_of_element_located
            case "visible":
                expected_condition = EC.visibility_of_element_located
            case "invisible":
                expected_condition = EC.invisibility_of_element_located
            case "selected":
                expected_condition = EC.element_located_to_be_selected
            case "text_present":
                expected_condition = EC.text_to_be_present_in_element
            case "frame_available":
                expected_condition = EC.frame_to_be_available_and_switch_to_it
            case _:
                expected_condition = EC.element_to_be_clickable
        return expected_condition

    def _get_expect_condition_multiple(self, expected_condition: Optional[str]) -> EC:
        """Get the expected condition for multiple elements based on the provided string.

        Args:
            expected_condition (Optional[str]): The expected condition type.

        Returns:
            EC: The expected condition object for multiple elements.
        """
        match expected_condition:
            case "invisible":
                expected_condition = EC.invisibility_of_element_located
            case "clickable":
                expected_condition = EC.element_to_be_clickable
            case "selected":
                expected_condition = EC.element_located_to_be_selected
            case "text_present":
                expected_condition = EC.text_to_be_present_in_element
            case "frame_available":
                expected_condition = EC.frame_to_be_available_and_switch_to_it
            case _:
                expected_condition = EC.presence_of_all_elements_located
        return expected_condition


class UIInteractions(Interactions):
    """Class for interacting with UI elements using Selenium WebDriver."""

    def _get_locator(self, locator: str) -> By:
        """Get the locator type based on the provided string.

        Args:
            locator (str): The locator type as a string.

        Returns:
            By: The locator type.
        """
        match locator:
            case "id":
                by = By.ID
            case "name":
                by = By.NAME
            case "class_name":
                by = By.CLASS_NAME
            case "tag_name":
                by = By.TAG_NAME
            case "xpath":
                by = By.XPATH
            case "link_text":
                by = By.LINK_TEXT
            case "partial_link_text":
                by = By.PARTIAL_LINK_TEXT
            case _:
                by = By.CSS_SELECTOR
        return by

    def get_element(
        self,
        element_value: str,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
        wait_time: Optional[int] = None,
    ) -> WebElement:
        """Get a single WebElement based on the expected condition, locator, and element_value.

        Args:
            element_value (str): The element value.
            locator (Optional[str]): The locator type.
            expected_condition (Optional[str]): The expected condition type.
            wait_time (Optional[int]): Time to wait for the condition.

        Returns:
            WebElement: The located WebElement.

        Raises:
            TimeoutException: If the element is not found within the wait time.
            NoSuchElementException: If the element is not found.
            WebDriverException: If a WebDriverException occurs.
        """
        if wait_time:
            self.wait_time = wait_time
        try:
            return WebDriverWait(self.driver, self.wait_time).until(
                self._get_expected_condition(expected_condition)(
                    (self._get_locator(locator), element_value)
                )
            )
        except TimeoutException as exc:
            self._take_error_screenshot()
            raise TimeoutException(
                f"Element with locator {locator} and value {element_value} not found."
            ) from exc
        except NoSuchElementException as exc:
            self._take_error_screenshot()
            raise NoSuchElementException(
                f"Element with locator {locator} and value {element_value} not found."
            ) from exc
        except WebDriverException as exc:
            self._take_error_screenshot()
            raise WebDriverException(f"WebDriverException occurred: {exc}") from exc

    def get_multiple_elements(
        self,
        element_value: str,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
        wait_time: Optional[int] = None,
    ) -> List[WebElement]:
        """Get a list of WebElements based on the expected condition, locator, and element_value.

        Args:
            element_value (str): The element value.
            locator (Optional[str]): The locator type.
            expected_condition (Optional[str]): The expected condition type.
            wait_time (Optional[int]): Time to wait for the condition.

        Returns:
            List[WebElement]: A list of located WebElements.

        Raises:
            TimeoutException: If the elements are not found within the wait time.
            NoSuchElementException: If the elements are not found.
            WebDriverException: If a WebDriverException occurs.
        """
        if wait_time:
            self.wait_time = wait_time
        try:
            return WebDriverWait(self.driver, self.wait_time).until(
                self._get_expect_condition_multiple(expected_condition)(
                    (self._get_locator(locator), element_value)
                )
            )
        except TimeoutException as exc:
            self._take_error_screenshot()
            raise TimeoutException(
                f"Element with locator {locator} and value {element_value} not found."
            ) from exc
        except NoSuchElementException as exc:
            self._take_error_screenshot()
            raise NoSuchElementException(
                f"Element with locator {locator} and value {element_value} not found."
            ) from exc
        except WebDriverException as exc:
            self._take_error_screenshot()
            raise WebDriverException(f"WebDriverException occurred: {exc}") from exc

    def get_text(
        self,
        element_value: str,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
    ) -> str:
        """Get the text of the WebElement based on the locator and expected condition.

        Args:
            element_value (str): The element value.
            locator (Optional[str]): The locator type.
            expected_condition (Optional[str]): The expected condition type.

        Returns:
            str: The text of the located WebElement.
        """
        return self.get_element(element_value, locator, expected_condition).text

    def get_table(
        self,
        element_value: str,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get the data from a table element.

        Args:
            element_value (str): The element value. This needs to be a valid table element.
            locator (Optional[str]): The locator type.
            expected_condition (Optional[str]): The expected condition type.

        Returns:
            pandas.DataFrame: A DataFrame containing the table data.
        """
        element = self.get_element(element_value, locator, expected_condition)
        return pd.read_html(StringIO(element.get_attribute("outerHTML")))[0]

    def get_value(
        self,
        element_value: str,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
    ) -> str:
        """Get the value attribute of the WebElement based on the locator and expected condition.

        Args:
            element_value (str): The element value.
            locator (Optional[str]): The locator type.
            expected_condition (Optional[str]): The expected condition type.

        Returns:
            str: The value attribute of the located WebElement.
        """
        return self.get_element(
            element_value, locator, expected_condition
        ).get_attribute("value")

    def press_button(
        self,
        element_value: str,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
    ) -> None:
        """Click on the WebElement based on the locator and expected condition.

        Args:
            element_value (str): The element value.
            locator (Optional[str]): The locator type.
            expected_condition (Optional[str]): The expected condition type.
        """
        element = self.get_element(element_value, locator, expected_condition)
        element.click()

    def enter_text(
        self,
        text: str,
        element_value: str,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
    ):
        """Populate the text field with the provided text.

        Args:
            text (str): The text to populate in the field.
            element_value (str): The element value.
            locator (Optional[str]): The locator type.
            expected_condition (Optional[str]): The expected condition type.
        """
        element = self.get_element(element_value, locator, expected_condition)
        element.clear()
        element.send_keys(text)

    def set_checkbox_state(
        self,
        state: bool,
        element_value: str,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
    ):
        """Set the state of a checkbox.

        Args:
            state (bool): True to check the checkbox, False to uncheck it.
            element_value (str): The element value.
            locator (Optional[str]): The locator type.
            expected_condition (Optional[str]): The expected condition type.
        """
        element = self.get_element(element_value, locator, expected_condition)
        if element.is_selected() != state:
            element.click()

    def set_select_option(
        self,
        option: str,
        element_value: str,
        select_type: str = None,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
    ):
        """Select an option from a dropdown.

        Args:
            option (str): The option to select. This can be the visible text,
            index, or value of the option. Default is by value.
            element_value (str): The element value.
            select_type (str): The type of selection to perform. Default is value.
            locator (Optional[str]): The locator type.
            expected_condition (Optional[str]): The expected condition type.
        """
        element = self.get_element(element_value, locator, expected_condition)
        select = Select(element)
        if select_type == "index":
            select.select_by_index(int(option))
        elif select_type == "visible_text":
            select.select_by_visible_text(option)
        else:
            select.select_by_value(option)

    def if_web_page_contains(
        self,
        element_value: str,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
    ) -> bool:
        """Check if the web page contains an element based on the locator and expected condition.

        Args:
            element_value (str): The element value.
            locator (Optional[str]): The locator type.
            expected_condition (Optional[str]): The expected condition type.

        Returns:
            bool: True if the element is found, False otherwise.
        """
        try:
            self.get_element(element_value, locator, expected_condition)
            return True
        except (TimeoutException, NoSuchElementException):
            return False

    def wait_for_element(
        self,
        element_value: str,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
        wait_time: Optional[int] = None,
    ) -> WebElement:
        """Wait for an element to be present based on the locator and expected condition.

        Args:
            element_value (str): The element value.
            locator (Optional[str]): The locator type.
            expected_condition (Optional[str]): The expected condition type.
            wait_time (Optional[int]): Time to wait for the element.

        Returns:
            WebElement: The located WebElement.
        """
        if wait_time:
            self.wait_time = wait_time
        return self.get_element(element_value, locator, expected_condition, wait_time)


class WEInteractions(Interactions):
    """Class for interacting with web elements directly using WebElement instances."""

    def get_text_we(self, web_element: WebElement) -> str:
        """Get the text of the WebElement directly.

        Args:
            web_element (WebElement): The WebElement to get text from.

        Returns:
            str: The text of the WebElement.
        """
        return web_element.text

    def get_table_we(self, web_element: WebElement) -> pd.DataFrame:
        """Get the data from a table element directly.

        Args:
            web_element (WebElement): The WebElement representing the table.

        Returns:
            pandas.DataFrame: A DataFrame containing the table data.
        """
        return pd.read_html(StringIO(web_element.get_attribute("outerHTML")))[0]

    def get_value_we(self, web_element: WebElement) -> str:
        """Get the value attribute of the WebElement directly.

        Args:
            web_element (WebElement): The WebElement to get value from.

        Returns:
            str: The value attribute of the WebElement.
        """
        return web_element.get_attribute("value")

    def press_button_we(self, web_element: WebElement) -> None:
        """Click on the WebElement directly.

        Args:
            web_element (WebElement): The WebElement to click.
        """
        web_element.click()

    def enter_text_we(self, text: str, web_element: WebElement):
        """Populate the text field with the provided text directly.

        Args:
            text (str): The text to populate in the field.
            web_element (WebElement): The WebElement to populate.
        """
        web_element.clear()
        web_element.send_keys(text)

    def set_checkbox_state_we(self, state: bool, web_element: WebElement):
        """Set the state of a checkbox directly.

        Args:
            state (bool): True to check the checkbox, False to uncheck it.
            web_element (WebElement): The WebElement representing the checkbox.
        """
        if web_element.is_selected() != state:
            web_element.click()

    def set_select_option_we(
        self, option: str, web_element: WebElement, select_type: str = None
    ):
        """Select an option from a dropdown directly.

        Args:
            option (str): The option to select. This can be the visible text,
            index, or value of the option. Default is by value.
            web_element (WebElement): The WebElement representing the dropdown.
            select_type (str): The type of selection to perform. Default is value.
        """
        select = Select(web_element)
        if select_type == "index":
            select.select_by_index(int(option))
        elif select_type == "visible_text":
            select.select_by_visible_text(option)
        else:
            select.select_by_value(option)

    def if_web_page_contains_we(
        self, web_element: WebElement, expected_condition: Optional[str] = None
    ) -> bool:
        """Check if the web page contains an element directly using WebElement
        and expected condition.

        Args:
            web_element (WebElement): The WebElement to check.
            expected_condition (Optional[str]): The expected condition type.

        Returns:
            bool: True if the element is found, False otherwise.
        """
        try:
            condition = self._get_expected_condition(expected_condition)
            return condition(web_element)(self.driver)
        except WebDriverException:
            return False

    def wait_for_element_we(
        self,
        web_element: WebElement,
        expected_condition: Optional[str] = None,
        wait_time: Optional[int] = None,
    ) -> WebElement:
        """Wait for an element to be present directly using WebElement and expected condition.

        Args:
            web_element (WebElement): The WebElement to wait for.
            expected_condition (Optional[str]): The expected condition type.
            wait_time (Optional[int]): Time to wait for the element.

        Returns:
            WebElement: The WebElement.
        """
        if wait_time:
            self.wait_time = wait_time
        condition = self._get_expected_condition(expected_condition)
        WebDriverWait(self.driver, self.wait_time).until(condition(web_element))
        return web_element
