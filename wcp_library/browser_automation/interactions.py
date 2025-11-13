"""
This module provides classes and methods for interacting with web elements using Selenium WebDriver.

The module contains the following classes:
- Interactions: A base class for common web interactions.
- UIInteractions: A subclass of Interactions for interacting with web elements using locators.
- WEInteractions: A subclass of Interactions for interacting with web elements directly.

Each class provides methods for performing various web interactions such as navigating to a URL,
taking screenshots, waiting for elements, clicking buttons, entering text, and more.
"""

import time
import logging
from datetime import datetime
from io import StringIO
from typing import List, Optional, Union

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


class BrowserInteractionError(Exception):
    """
    Exception raised when an Seemium interaction with the browser fails.
    """


class Interactions:
    """Class for interacting with web elements using Selenium WebDriver.

    Attributes:
        driver: The Selenium WebDriver instance.
    """

    def __init__(self, driver):
        self.driver = driver
        logging.basicConfig(level=logging.INFO)

    def take_screenshot(self, file_path: str):
        """Take a screenshot of the current page and save it to the specified file path.

        :param file_path: The path where the screenshot will be saved.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        if self.driver:
            self.driver.save_screenshot(file_path)
        else:
            raise RuntimeError("WebDriver is not initialized.")

    def _take_error_screenshot(self) -> None:
        """
        Take a screenshot of the current page and save it to the P drive.

        :return: None
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        self.take_screenshot(
            f"{EXECUTION_ERROR_SCREENSHOT_FOLDER}/Failure Screenshot - {datetime.now().strftime('%Y-%m-%d_%H-%M')}.png"
        )

    def _get_expect_condition_multiple(self, expected_condition: Optional[str]) -> EC:
        """Get the expected condition for multiple elements based on the provided string.

        :param expected_condition: The expected condition type.
        :return: The expected condition object for multiple elements.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        return (
            EC.presence_of_all_elements_located
            if expected_condition == "present"
            else EC.visibility_of_all_elements_located
        )

    def _get_wait_time(self, wait_time: float) -> float:
        """If a wait time has been specified it is returned. Otherwise, the wait time comes from
        the implicit timeout set when initializing the browser. That value is in milliseconds so
        it is divided by 1000 (WebDriverWait expects a float number in seconds)

        :param wait_time: The wait time in seconds.
        :return: The wait time in seconds.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        return int(
            wait_time
            or (
                getattr(self, "browser_options", {})
                .get("timeouts", {})
                .get("implicit", 0)
            )
            / 1000
        )  # Timeouts are in ms


class UIInteractions(Interactions):
    """Class for interacting with UI elements using Selenium WebDriver."""

    def _get_locator(self, locator: str) -> str:
        """Get the locator type based on the provided string.

        :param locator: The locator type.
        :return: The locator type.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        match locator:
            case "id":
                by = By.ID
            case "name":
                by = By.NAME
            case "class":
                by = By.CLASS_NAME
            case "tag":
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

    def _get_expected_condition(self, expected_condition: Optional[str]) -> EC:
        """Get the expected condition based on the provided string.

        :param expected_condition: The expected condition type.
        :return: The expected condition object.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        match expected_condition:
            case "present":
                expected_condition = EC.presence_of_element_located
            case "visible":
                expected_condition = EC.visibility_of_element_located
            case "selected":
                expected_condition = EC.element_located_to_be_selected
            case "frame_available":
                expected_condition = EC.frame_to_be_available_and_switch_to_it
            case _:
                expected_condition = EC.element_to_be_clickable
        return expected_condition

    def get_element(
        self,
        element_value: str,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
        wait_time: Optional[float] = 0,
    ) -> WebElement:
        """Get a single WebElement based on the expected condition, locator, and element_value.

        :param element_value: The expected element value.
        :param locator: The locator type.
            Options: 'css'(Default), 'id', 'name', 'class', 'tag',
            'xpath', 'link_text', 'partial_link_text'
        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'present', 'visible',
            'selected', 'frame_available'
        :param wait_time: Time to wait for the condition.
        :return: The located WebElement.
        :raises RuntimeError: If the WebDriver is not initialized.
        :raises TimeoutException: If the element is not found within the wait time.
        :raises NoSuchElementException: If the element is not found.
        :raises WebDriverException: If a WebDriverException occurs.
        """

        try:
            return WebDriverWait(self.driver, self._get_wait_time(wait_time)).until(
                self._get_expected_condition(expected_condition)(
                    (self._get_locator(locator), element_value)
                )
            )

        except WebDriverException:
            self._take_error_screenshot()
            raise

    def get_multiple_elements(
        self,
        element_value: str,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
        wait_time: Optional[float] = 0,
    ) -> List[WebElement]:
        """Get a list of WebElements based on the expected condition, locator, and element_value.

        :param element_value: The expected element value.
        :param locator: The locator type.
            Options: 'css'(Default), 'id', 'name', 'class', 'tag',
            'xpath', 'link_text', 'partial_link_text'
        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'present', 'visible',
            'selected', 'frame_available'
        :param wait_time: Time to wait for the condition.
        :return: A list of located WebElements.
        :raises RuntimeError: If the WebDriver is not initialized.
        :raises TimeoutException: If the elements are not found within the wait time.
        :raises NoSuchElementException: If the elements are not found.
        :raises WebDriverException: If a WebDriverException occurs.
        """

        try:
            return WebDriverWait(self.driver, self._get_wait_time(wait_time)).until(
                self._get_expect_condition_multiple(expected_condition)(
                    (self._get_locator(locator), element_value)
                )
            )
        except WebDriverException:
            self._take_error_screenshot()
            raise

    def get_first_element(
        self,
        elements: list[dict],
        wait_time: Optional[float] = 0,
    ) -> WebElement:
        """Get the first available WebElement from a list of element dictionaries.

        Each dictionary can contain:
            - "element": the element value (required)
            - "locator": the locator type (optional, defaults to "css")
            - "expected_condition": the condition to wait for (optional, defaults to "clickable")

        :param elements: List of dictionaries defining elements.
        :param wait_time: The wait time in seconds.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        # Normalize each dictionary to (value, locator, condition)
        normalized = []
        for item in elements:
            element_value = item.get("element")
            if not element_value:
                raise ValueError(f"Missing 'element' key in: {item}")
            locator = item.get("locator") or "css"
            condition = item.get("expected_condition") or "clickable"
            normalized.append((element_value, locator, condition))

        end_time = time.time() + self._get_wait_time(wait_time)
        while time.time() < end_time:
            for element_value, locator, condition in normalized:
                try:
                    return self.get_element(element_value, locator, condition)
                except WebDriverException:
                    continue
        raise TimeoutException("No element became clickable within the timeout.")

    def get_text(
        self,
        element_value: str,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
        wait_time: Optional[float] = 0,
    ) -> str:
        """Get the text of the WebElement based on the locator and expected condition.

        :param element_value: The value used to identify the element.
        :param locator: The locator type.
            Options: 'css'(Default), 'id', 'name', 'class', 'tag',
            'xpath', 'link_text', 'partial_link_text'
        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'present', 'visible',
            'selected', 'frame_available'
        :param wait_time: Time to wait for the condition.
        :return: The text of the located WebElement.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        return self.get_element(
            element_value, locator, expected_condition, wait_time
        ).text

    def get_table(
        self,
        element_value: str,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
        wait_time: Optional[float] = 0,
    ) -> pd.DataFrame:
        """Get the data from a table element.

        :param element_value: The value used to identify the element.
        :param locator: The locator type.
            Options: 'css'(Default), 'id', 'name', 'class', 'tag',
            'xpath', 'link_text', 'partial_link_text'
        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'present', 'visible',
            'selected', 'frame_available'
        :param wait_time: Time to wait for the condition.
        :return: The data from the table element.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        element = self.get_element(
            element_value, locator, expected_condition, wait_time
        )
        return pd.read_html(StringIO(element.get_attribute("outerHTML")))[0]

    def get_value(
        self,
        element_value: str,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
        wait_time: Optional[float] = 0,
    ) -> str:
        """Get the value attribute of the WebElement based on the locator and expected condition.

        :param element_value: The value used to identify the element.
        :param locator: The locator type.
            Options: 'css'(Default), 'id', 'name', 'class', 'tag',
            'xpath', 'link_text', 'partial_link_text'
        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'present', 'visible',
            'selected', 'frame_available'
        :param wait_time: Time to wait for the condition.
        :return: The value of the located WebElement.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        return self.get_element(
            element_value, locator, expected_condition, wait_time
        ).get_attribute("value")

    def press_button(
        self,
        element_value: str,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
        wait_time: Optional[float] = 0,
    ) -> None:
        """Click on the WebElement based on the locator and expected condition.

        :param element_value: The value used to identify the element.
        :param locator: The locator type.
            Options: 'css'(Default), 'id', 'name', 'class', 'tag',
            'xpath', 'link_text', 'partial_link_text'
        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'present', 'visible',
            'selected', 'frame_available'
        :param wait_time: Time to wait for the condition.
        :return:
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        element = self.get_element(
            element_value, locator, expected_condition, wait_time
        )
        element.click()

    def enter_text(
        self,
        text: str,
        element_value: str,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
        wait_time: Optional[float] = 0,
    ) -> None:
        """Populate the text field with the provided text.

        :param text: The text to enter.
        :param element_value: The value used to identify the element.
        :param locator: The locator type.
            Options: 'css'(Default), 'id', 'name', 'class', 'tag',
            'xpath', 'link_text', 'partial_link_text'
        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'present', 'visible',
            'selected', 'frame_available'
        :param wait_time: Time to wait for the condition.
        :return:
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        element = self.get_element(
            element_value, locator, expected_condition, wait_time
        )
        try:
            element.clear()
        except WebDriverException:
            pass
        element.send_keys(str(text))

    def set_checkbox_state(
        self,
        state: bool,
        element_value: str,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
        wait_time: Optional[float] = 0,
    ) -> None:
        """Set the state of a checkbox.

        :param state: The state to set.
        :param element_value: The value used to identify the element.
        :param locator: The locator type.
            Options: 'css'(Default), 'id', 'name', 'class', 'tag',
            'xpath', 'link_text', 'partial_link_text'
        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'present', 'visible',
            'selected', 'frame_available'
        :param wait_time: Time to wait for the condition.
        :return:
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        element = self.get_element(
            element_value, locator, expected_condition, wait_time
        )
        if element.is_selected() != state:
            element.click()

    def set_select_option(
        self,
        option: str,
        element_value: str,
        select_type: str = None,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
        wait_time: Optional[float] = 0,
    ) -> None:
        """Select an option from a dropdown.

        :param option: The option to select. This can be the visible text, index, or value of the option.
        :param element_value: The value used to identify the element.
        :param select_type: The type of selection to perform.
            Options: 'value'(Default), 'index', 'visible_text'
        :param locator: The locator type.
            Options: 'css'(Default), 'id', 'name', 'class', 'tag',
            'xpath', 'link_text', 'partial_link_text'
        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'present', 'visible',
            'selected', 'frame_available'
        :param wait_time: Time to wait for the condition.
        :return:
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        element = self.get_element(
            element_value, locator, expected_condition, wait_time
        )
        select = Select(element)
        if select_type == "index":
            select.select_by_index(int(option))
        elif select_type == "visible_text":
            select.select_by_visible_text(option)
        else:
            select.select_by_value(option)

    def web_page_contains(
        self,
        element_value: str,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
        wait_time: Optional[float] = 0,
    ) -> Union[WebElement, bool]:
        """
        Determine whether a web element is present on the page based on
        the provided locator and expected condition.

        :param element_value: The value used to identify the element.
        :param locator: The locator type.
            Options: 'css'(Default), 'id', 'name', 'class', 'tag',
            'xpath', 'link_text', 'partial_link_text'
        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'present', 'visible',
            'selected', 'frame_available'
        :param wait_time: Time to wait for the condition.
        :return: The located WebElement if found; otherwise, False if the element is not present
        or an exception occurs.
        :raises RuntimeError: If the WebDriver is not initialized.
        """
        try:
            return WebDriverWait(self.driver, self._get_wait_time(wait_time)).until(
                self._get_expected_condition(expected_condition)(
                    (self._get_locator(locator), element_value)
                )
            )
        except WebDriverException:
            return False

    def wait_for_element(
        self,
        element_value: str,
        locator: Optional[str] = None,
        expected_condition: Optional[str] = None,
        wait_time: Optional[float] = 0,
    ) -> WebElement:
        """
        Wait for an element to be present based on the locator and expected condition.

        :param element_value: The value used to identify the element.
        :param locator: The locator type.
            Options: 'css'(Default), 'id', 'name', 'class', 'tag',
            'xpath', 'link_text', 'partial_link_text'
        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'present', 'visible',
            'selected', 'frame_available'
        :param wait_time: Time to wait for the element.
        :return: The located WebElement.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        return self.get_element(
            element_value, locator, expected_condition, self._get_wait_time(wait_time)
        )

    def text_is_present(
            self,
            text: str,
            element_value: str,
            locator: Optional[str] = None,
            text_location: Optional[str] = None,
            wait_time: Optional[float] = 0,
        ) -> bool:
            """
            Checks whether the specified text is present within a web element.

            :param text: The text to verify within the element.
            :param element_value: The value used to identify the element.
            :param locator: The locator type.
                Options: 'css'(Default), 'id', 'name', 'class', 'tag',
                'xpath', 'link_text', 'partial_link_text'
            :param text_location: Where in the element to look for the text.
                Options: 'anywhere'(Default), 'attribute', 'value'
            :param wait_time: Time to wait for the condition.
            :return: True if the text is found within the element, False otherwise.
            :raises RuntimeError: If the WebDriver is not initialized.
            """

            expected_condition = EC.text_to_be_present_in_element
            if text_location == "attribute":
                expected_condition = EC.text_to_be_present_in_element_attribute
            elif text_location == "value":
                expected_condition = EC.text_to_be_present_in_element_value

            return WebDriverWait(self.driver, self._get_wait_time(wait_time)).until(
                expected_condition((self._get_locator(locator), element_value), text)
            )


class WEInteractions(Interactions):
    """Class for interacting with web elements directly using WebElement instances."""

    def _get_expected_condition_we(self, expected_condition: Optional[str] = None):
        """
        Return an expected condition that accepts a WebElement.

        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'visible', 'selected', 'staleness'
        :return: A Selenium expected condition function.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        match expected_condition:
            case "visible":
                expected_condition = EC.visibility_of
            case "invisible":
                expected_condition = EC.invisibility_of_element
            case "selected":
                expected_condition = EC.element_to_be_selected
            case "staleness":
                expected_condition = EC.staleness_of
            case _:
                expected_condition = EC.element_to_be_clickable
        return expected_condition

    def get_text_we(
        self,
        web_element: WebElement,
        expected_condition: Optional[str] = None,
        wait_time: Optional[float] = 0,
    ) -> str:
        """
        Get the text of the WebElement directly.

        :param web_element: The WebElement to get text from.
        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'visible', 'selected', 'staleness'
        :param wait_time: Time to wait for the element.
        :return: The text of the WebElement.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        web_element = self.wait_for_element_we(
            web_element, expected_condition, wait_time
        )
        return web_element.text

    def get_table_we(
        self,
        web_element: WebElement,
        expected_condition: Optional[str] = None,
        wait_time: Optional[float] = 0,
    ) -> pd.DataFrame:
        """
        Get the data from a table element directly.

        :param web_element: The WebElement representing the table.
        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'visible', 'selected', 'staleness'
        :param wait_time: Time to wait for the element.
        :return: A DataFrame containing the table data.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        web_element = self.wait_for_element_we(
            web_element, expected_condition, wait_time
        )
        return pd.read_html(StringIO(web_element.get_attribute("outerHTML")))[0]

    def get_value_we(
        self,
        web_element: WebElement,
        expected_condition: Optional[str] = None,
        wait_time: Optional[float] = 0,
    ) -> str:
        """
        Get the value attribute of the WebElement directly.

        :param web_element: The WebElement to get value from.
        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'visible', 'selected', 'staleness'
        :param wait_time: Time to wait for the element.
        :return: The value attribute of the WebElement.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        web_element = self.wait_for_element_we(
            web_element, expected_condition, wait_time
        )
        return web_element.get_attribute("value")

    def press_button_we(
        self,
        web_element: WebElement,
        expected_condition: Optional[str] = None,
        wait_time: Optional[float] = 0,
    ) -> None:
        """Click on the WebElement directly.

        :param web_element: The WebElement to click.
        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'visible', 'selected', 'staleness'
        :param wait_time: Time to wait for the element.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        web_element = self.wait_for_element_we(
            web_element, expected_condition, wait_time
        )
        web_element.click()

    def enter_text_we(
        self,
        text: str,
        web_element: WebElement,
        expected_condition: Optional[str] = None,
        wait_time: Optional[float] = 0,
    ) -> None:
        """Populate the text field with the provided text directly.

        :param text: The text to enter.
        :param web_element: The WebElement to populate.
        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'visible', 'selected', 'staleness'
        :param wait_time: Time to wait for the element.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        web_element = self.wait_for_element_we(
            web_element, expected_condition, wait_time
        )
        web_element.clear()
        web_element.send_keys(text)

    def set_checkbox_state_we(
        self,
        state: bool,
        web_element: WebElement,
        expected_condition: Optional[str] = None,
        wait_time: Optional[float] = 0,
    ) -> None:
        """Set the state of a checkbox directly.

        :param state: The state to set. True to check the checkbox, False to uncheck it.
        :param web_element: The WebElement representing the checkbox.
        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'visible', 'selected', 'staleness'
        :param wait_time: Time to wait for the element.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        web_element = self.wait_for_element_we(
            web_element, expected_condition, wait_time
        )
        if web_element.is_selected() != state:
            web_element.click()

    def set_select_option_we(
        self,
        option: str,
        web_element: WebElement,
        select_type: str = None,
        expected_condition: Optional[str] = None,
        wait_time: Optional[float] = 0,
    ) -> None:
        """Select an option from a dropdown directly.

        :param option: The option to select.
            This can be the visible text, index, or value of the option.
            Default is by value.
        :param web_element: The WebElement representing the dropdown.
        :param select_type: The type of selection to perform.
            Options: 'value' (default), 'index', 'visible_text'
        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'visible', 'selected', 'staleness'
        :param wait_time: Time to wait for the element.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        web_element = self.wait_for_element_we(
            web_element, expected_condition, wait_time
        )
        select = Select(web_element)
        if select_type == "index":
            select.select_by_index(int(option))
        elif select_type == "visible_text":
            select.select_by_visible_text(option)
        else:
            select.select_by_value(option)

    def web_page_contains_we(
        self,
        web_element: WebElement,
        expected_condition: Optional[str] = None,
        wait_time: Optional[float] = 0,
    ) -> WebElement | bool:
        """Check if the web page contains an element directly using WebElement
        and expected condition.

        :param web_element: The WebElement to check.
        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'visible', 'selected', 'staleness'
        :param wait_time: Time to wait for the condition.
        :return: The located WebElement if found; otherwise, False if the element is not present
        or an exception occurs.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        try:
            return self.wait_for_element_we(
                web_element, expected_condition, self._get_wait_time(wait_time)
            )
        except (TimeoutException, NoSuchElementException):
            return False

    def wait_for_element_we(
        self,
        web_element: WebElement,
        expected_condition: Optional[str] = None,
        wait_time: Optional[float] = 0,
    ) -> WebElement:
        """
        Wait for an element to be present directly using WebElement and expected condition.

        :param web_element: The WebElement to wait for.
        :param expected_condition: The expected condition type.
            Options: 'clickable'(Default), 'visible', 'selected', 'staleness'
        :param wait_time: Time to wait for the element.
        :return: The WebElement if it meets the expected condition.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        condition = self._get_expected_condition_we(expected_condition)
        WebDriverWait(self.driver, self._get_wait_time(wait_time)).until(
            condition(web_element)
        )
        return web_element
