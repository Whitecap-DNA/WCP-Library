"""
Web element interaction utilities for Selenium WebDriver.

This module provides classes for interacting with web elements using
Selenium WebDriver. It supports both locator-based and direct WebElement-based
interaction patterns.

Classes
-------
Interactions
    Base class providing common web interaction utilities.
UIInteractions
    Locator-based element interactions (find by CSS, XPath, ID, etc.).
WEInteractions
    Direct WebElement-based interactions.
"""

import logging
import time
from datetime import datetime
from io import StringIO
from pathlib import Path

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

from wcp_library.graph import get_headers
from wcp_library.graph.sharepoint import upload_file

logger = logging.getLogger(__name__)

# ======================================================================
# Base class
# ======================================================================


class Interactions:
    """
    Base class for web element interactions using Selenium WebDriver.

    Provides shared utilities for screenshots and wait-time resolution used
    by both ``UIInteractions`` and ``WEInteractions``.

    :param driver: An initialised Selenium WebDriver instance.
    :param sharepoint_config: Configuration for uploading error screenshots to
        SharePoint. Expected keys: ``headers``, ``site_id``, ``file_path``.
    """

    def __init__(
        self,
        driver,
        sharepoint_config: dict[str, str] | None = None,
    ) -> None:
        self.driver = driver
        self.sharepoint_config = sharepoint_config

    # ------------------------------------------------------------------
    # Screenshots
    # ------------------------------------------------------------------

    def take_screenshot(self, file_path: Path) -> None:
        """
        Save a screenshot of the current page.

        :param file_path: Destination path for the screenshot file.
        :raises RuntimeError: If the WebDriver is not initialised.
        """
        if self.driver:
            self.driver.save_screenshot(file_path)
        else:
            raise RuntimeError("WebDriver is not initialized.")

    def _take_error_screenshot(self) -> None:
        """
        Capture an error screenshot.

        If ``sharepoint_config`` is set the image is uploaded to SharePoint;
        otherwise it is saved to the default local folder.
        """
        filename = f"{datetime.now().strftime('%Y-%m-%d_%H-%M')}.png"

        if self.sharepoint_config:
            screenshot_bytes = self.driver.get_screenshot_as_png()
            headers = get_headers(
                self.sharepoint_config["app_id"],
                self.sharepoint_config["app_secret"],
                self.sharepoint_config["tenant_id"],
            )
            upload_file(
                headers=headers,
                site_id=self.sharepoint_config["site_id"],
                file_path="/Automation/.Execution Error Screenshots",
                filename=filename,
                content=screenshot_bytes,
            )
        else:
            screenshot_folder = Path("Execution Error Screenshots")
            screenshot_folder.mkdir(exist_ok=True, parents=True)
            self.take_screenshot(screenshot_folder / filename)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_wait_time(self, wait_time: float | None = 0) -> float:
        """
        Return the effective wait time in seconds.

        If *wait_time* is non-zero it is returned directly. Otherwise the
        implicit timeout from ``browser_options`` is used (converted from
        milliseconds to seconds).

        :param wait_time: Explicit wait time in seconds. Defaults to ``0``.
        :return: Wait time in seconds.
        """
        if wait_time:
            return int(wait_time)

        implicit_ms = (
            getattr(self, "browser_options", {}).get("timeouts", {}).get("implicit", 0)
        )
        return int(implicit_ms / 1000)


# ======================================================================
# Locator-based interactions
# ======================================================================

_LOCATOR_MAP: dict[str, str] = {
    "id": By.ID,
    "name": By.NAME,
    "class": By.CLASS_NAME,
    "tag": By.TAG_NAME,
    "xpath": By.XPATH,
    "link_text": By.LINK_TEXT,
    "partial_link_text": By.PARTIAL_LINK_TEXT,
}

_SINGLE_EC_MAP: dict[str, type] = {
    "present": EC.presence_of_element_located,
    "visible": EC.visibility_of_element_located,
    "selected": EC.element_located_to_be_selected,
    "frame_available": EC.frame_to_be_available_and_switch_to_it,
}

_MULTIPLE_EC_MAP: dict[str, type] = {
    "present": EC.presence_of_all_elements_located,
}


class UIInteractions(Interactions):
    """
    Locator-based web element interactions.

    All methods accept a string *element_value* together with a *locator*
    alias (e.g. ``'xpath'``, ``'id'``, ``'css'``) and resolve the target
    element via ``WebDriverWait``.

    :param driver: An initialised Selenium WebDriver instance.
    :param sharepoint_config: Configuration for uploading error screenshots to
        SharePoint.
    """

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_locator(locator: str | None) -> str:
        """
        Resolve a locator alias to a Selenium ``By`` constant.

        :param locator: One of ``'id'``, ``'name'``, ``'class'``, ``'tag'``,
            ``'xpath'``, ``'link_text'``, ``'partial_link_text'``, or ``None`` / any
            other value for CSS selector (default).
        :return: The corresponding ``By`` constant.
        """
        return _LOCATOR_MAP.get(locator, By.CSS_SELECTOR)

    @staticmethod
    def _get_expected_condition(expected_condition: str | None):
        """
        Resolve a single-element expected-condition alias.

        :param expected_condition: One of ``'present'``, ``'visible'``, ``'selected'``,
            ``'frame_available'``, or ``None`` / any other value for *clickable*
            (default).
        :return: A Selenium expected-condition class.
        """
        return _SINGLE_EC_MAP.get(expected_condition, EC.element_to_be_clickable)

    @staticmethod
    def _get_expected_condition_multiple(expected_condition: str | None):
        """
        Resolve a multi-element expected-condition alias.

        :param expected_condition: ``'present'`` for presence, anything else for
            visibility (default).
        :return: A Selenium expected-condition class for multiple elements.
        """
        return _MULTIPLE_EC_MAP.get(
            expected_condition, EC.visibility_of_all_elements_located
        )

    # ------------------------------------------------------------------
    # Element retrieval
    # ------------------------------------------------------------------

    def get_element(
        self,
        element_value: str,
        locator: str | None = None,
        expected_condition: str | None = None,
        wait_time: float | None = 0,
    ) -> WebElement:
        """
        Locate a single element.

        :param element_value: Selector or identifier for the element.
        :param locator: Locator strategy. One of ``'css'`` (default), ``'id'``,
            ``'name'``, ``'class'``, ``'tag'``, ``'xpath'``, ``'link_text'``,
            ``'partial_link_text'``.
        :param expected_condition: Wait condition. One of ``'clickable'`` (default),
            ``'present'``, ``'visible'``, ``'selected'``, ``'frame_available'``.
        :param wait_time: Seconds to wait for the condition.
        :return: The located element.
        :raises TimeoutException: If the element is not found within *wait_time*.
        :raises NoSuchElementException: If the element does not exist.
        :raises WebDriverException: On any other WebDriver error (an error screenshot is
            taken).
        """
        try:
            return WebDriverWait(self.driver, self._get_wait_time(wait_time)).until(
                self._get_expected_condition(expected_condition)(
                    (self._get_locator(locator), element_value)
                )
            )
        except WebDriverException:
            logger.exception(
                "Failed to locate element: element_value=%s, locator=%s, expected_condition=%s, wait_time=%s",
                element_value,
                locator,
                expected_condition,
                wait_time,
            )
            self._take_error_screenshot()
            raise

    def get_multiple_elements(
        self,
        element_value: str,
        locator: str | None = None,
        expected_condition: str | None = None,
        wait_time: float | None = 0,
    ) -> list[WebElement]:
        """
        Locate multiple elements matching the selector.

        :param element_value: Selector or identifier for the elements.
        :param locator: Locator strategy (see ``get_element``).
        :param expected_condition: Wait condition. ``'present'`` or ``'visible'``
            (default).
        :param wait_time: Seconds to wait for the condition.
        :return: The located elements.
        :raises TimeoutException: If no elements are found within *wait_time*.
        :raises WebDriverException: On any other WebDriver error (an error screenshot is
            taken).
        """
        try:
            return WebDriverWait(self.driver, self._get_wait_time(wait_time)).until(
                self._get_expected_condition_multiple(expected_condition)(
                    (self._get_locator(locator), element_value)
                )
            )
        except WebDriverException:
            return []

    def get_first_element(
        self,
        elements: list[dict],
        wait_time: float | None = 0,
    ) -> WebElement:
        """
        Return the first available element from a list of candidates.

        :param elements: Each dictionary must contain an ``'element'`` key and may
            optionally contain ``'locator'`` (default ``'css'``) and
            ``'expected_condition'`` (default ``'clickable'``).
        :param wait_time: Total seconds to keep polling across all candidates.
        :return: The first element that satisfies its expected condition.
        :raises ValueError: If any dictionary is missing the ``'element'`` key.
        :raises TimeoutException: If no element becomes available within *wait_time*.
        """
        normalized: list[tuple[str, str, str]] = []

        for item in elements:
            value = item.get("element")
            if not value:
                raise ValueError(f"Missing 'element' key in: {item}")

            normalized.append(
                (
                    value,
                    item.get("locator", "css"),
                    item.get("expected_condition", "clickable"),
                )
            )

        deadline = time.time() + self._get_wait_time(wait_time)
        while time.time() < deadline:
            for value, loc, cond in normalized:
                el = self.web_page_contains(value, loc, cond)
                if el:
                    return el

        raise TimeoutException(
            f"Failed to locate any element. Candidates were: {normalized}"
        )

    # ------------------------------------------------------------------
    # Reading values
    # ------------------------------------------------------------------

    def get_text(
        self,
        element_value: str,
        locator: str | None = None,
        expected_condition: str | None = None,
        wait_time: float | None = 0,
    ) -> str:
        """
        Get the visible text of an element.

        :param element_value: Selector or identifier for the element.
        :param locator: Locator strategy (see ``get_element``).
        :param expected_condition: Wait condition (see ``get_element``).
        :param wait_time: Seconds to wait for the condition.
        :return: The element's visible text.
        """
        return self.get_element(
            element_value, locator, expected_condition, wait_time
        ).text

    def get_value(
        self,
        element_value: str,
        locator: str | None = None,
        expected_condition: str | None = None,
        wait_time: float | None = 0,
    ) -> str:
        """
        Get the ``value`` attribute of an element.

        :param element_value: Selector or identifier for the element.
        :param locator: Locator strategy (see ``get_element``).
        :param expected_condition: Wait condition (see ``get_element``).
        :param wait_time: Seconds to wait for the condition.
        :return: The element's ``value`` attribute.
        """
        return self.get_element(
            element_value, locator, expected_condition, wait_time
        ).get_attribute("value")

    def get_table(
        self,
        element_value: str,
        locator: str | None = None,
        expected_condition: str | None = None,
        wait_time: float | None = 0,
    ) -> pd.DataFrame:
        """
        Parse an HTML ``<table>`` element into a DataFrame.

        :param element_value: Selector or identifier for the table element.
        :param locator: Locator strategy (see ``get_element``).
        :param expected_condition: Wait condition (see ``get_element``).
        :param wait_time: Seconds to wait for the condition.
        :return: The table data.
        """
        element = self.get_element(
            element_value, locator, expected_condition, wait_time
        )
        return pd.read_html(StringIO(element.get_attribute("outerHTML")))[0]

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def press_button(
        self,
        element_value: str,
        locator: str | None = None,
        expected_condition: str | None = None,
        wait_time: float | None = 0,
    ) -> None:
        """
        Click an element.

        :param element_value: Selector or identifier for the element.
        :param locator: Locator strategy (see ``get_element``).
        :param expected_condition: Wait condition (see ``get_element``).
        :param wait_time: Seconds to wait for the condition.
        """
        self.get_element(element_value, locator, expected_condition, wait_time).click()

    def enter_text(
        self,
        text: str,
        element_value: str,
        locator: str | None = None,
        expected_condition: str | None = None,
        wait_time: float | None = 0,
    ) -> None:
        """
        Clear and populate a text field.

        :param text: The text to enter.
        :param element_value: Selector or identifier for the element.
        :param locator: Locator strategy (see ``get_element``).
        :param expected_condition: Wait condition (see ``get_element``).
        :param wait_time: Seconds to wait for the condition.
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
        locator: str | None = None,
        expected_condition: str | None = None,
        wait_time: float | None = 0,
    ) -> None:
        """
        Set a checkbox to the desired state.

        :param state: ``True`` to check, ``False`` to uncheck.
        :param element_value: Selector or identifier for the checkbox element.
        :param locator: Locator strategy (see ``get_element``).
        :param expected_condition: Wait condition (see ``get_element``).
        :param wait_time: Seconds to wait for the condition.
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
        select_type: str | None = None,
        locator: str | None = None,
        expected_condition: str | None = None,
        wait_time: float | None = 0,
    ) -> None:
        """
        Choose an option from a ``<select>`` dropdown.

        :param option: The option to select (text, index, or value depending on
            *select_type*).
        :param element_value: Selector or identifier for the ``<select>`` element.
        :param select_type: Selection strategy: ``'value'`` (default), ``'index'``, or
            ``'visible_text'``.
        :param locator: Locator strategy (see ``get_element``).
        :param expected_condition: Wait condition (see ``get_element``).
        :param wait_time: Seconds to wait for the condition.
        """
        element = self.get_element(
            element_value, locator, expected_condition, wait_time
        )
        select = Select(element)
        match select_type:
            case "index":
                select.select_by_index(int(option))
            case "visible_text":
                select.select_by_visible_text(option)
            case _:
                select.select_by_value(option)

    # ------------------------------------------------------------------
    # Presence / waiting
    # ------------------------------------------------------------------

    def web_page_contains(
        self,
        element_value: str,
        locator: str | None = None,
        expected_condition: str | None = None,
        wait_time: float | None = 0,
    ) -> WebElement | bool:
        """
        Check whether an element is present on the page.

        :param element_value: Selector or identifier for the element.
        :param locator: Locator strategy (see ``get_element``).
        :param expected_condition: Wait condition (see ``get_element``).
        :param wait_time: Seconds to wait for the condition.
        :return: The element if found, otherwise ``False``.
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
        locator: str | None = None,
        expected_condition: str | None = None,
        wait_time: float | None = 0,
    ) -> WebElement:
        """
        Block until an element meets the expected condition.

        :param element_value: Selector or identifier for the element.
        :param locator: Locator strategy (see ``get_element``).
        :param expected_condition: Wait condition (see ``get_element``).
        :param wait_time: Seconds to wait for the element.
        :return: The located element.
        """
        return self.get_element(
            element_value,
            locator,
            expected_condition,
            self._get_wait_time(wait_time),
        )

    def text_is_present(
        self,
        text: str,
        element_value: str,
        locator: str | None = None,
        text_location: str | None = None,
        wait_time: float | None = 0,
    ) -> WebElement | bool:
        """
        Check whether *text* appears within an element.

        :param text: The text to search for.
        :param element_value: Selector or identifier for the element.
        :param locator: Locator strategy (see ``get_element``).
        :param text_location: Where to look: ``'anywhere'`` (default), ``'attribute'``,
            or ``'value'``.
        :param wait_time: Seconds to wait for the condition.
        :return: The element if the text is found, otherwise ``False``.
        """
        match text_location:
            case "attribute":
                condition = EC.text_to_be_present_in_element_attribute
            case "value":
                condition = EC.text_to_be_present_in_element_value
            case _:
                condition = EC.text_to_be_present_in_element

        try:
            return WebDriverWait(self.driver, self._get_wait_time(wait_time)).until(
                condition((self._get_locator(locator), element_value), text)
            )
        except TimeoutException:
            return False


# ======================================================================
# WebElement-based interactions
# ======================================================================

_WE_EC_MAP: dict[str, type] = {
    "visible": EC.visibility_of,
    "invisible": EC.invisibility_of_element,
    "selected": EC.element_to_be_selected,
    "staleness": EC.staleness_of,
}


class WEInteractions(Interactions):
    """
    Direct WebElement-based interactions.

    Methods accept an already-located ``WebElement`` rather than a locator
    string, which is useful when elements have already been retrieved or
    when working inside Shadow DOMs.

    :param driver: An initialised Selenium WebDriver instance.
    :param sharepoint_config: Configuration for uploading error screenshots to
        SharePoint.
    """

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_expected_condition_we(expected_condition: str | None = None):
        """
        Resolve a WebElement-based expected-condition alias.

        :param expected_condition: One of ``'visible'``, ``'invisible'``,
            ``'selected'``, ``'staleness'``, or ``None`` / any other value for
            *clickable* (default).
        :return: A Selenium expected-condition class.
        """
        return _WE_EC_MAP.get(expected_condition, EC.element_to_be_clickable)

    # ------------------------------------------------------------------
    # Waiting
    # ------------------------------------------------------------------

    def wait_for_element_we(
        self,
        web_element: WebElement,
        expected_condition: str | None = None,
        wait_time: float | None = 0,
    ) -> WebElement:
        """
        Block until a WebElement meets the expected condition.

        :param web_element: The element to wait on.
        :param expected_condition: Wait condition. One of ``'clickable'`` (default),
            ``'visible'``, ``'invisible'``, ``'selected'``, ``'staleness'``.
        :param wait_time: Seconds to wait.
        :return: The same element once the condition is met.
        """
        condition = self._get_expected_condition_we(expected_condition)
        WebDriverWait(self.driver, self._get_wait_time(wait_time)).until(
            condition(web_element)
        )
        return web_element

    # ------------------------------------------------------------------
    # Reading values
    # ------------------------------------------------------------------

    def get_text_we(
        self,
        web_element: WebElement,
        expected_condition: str | None = None,
        wait_time: float | None = 0,
    ) -> str:
        """
        Get the visible text of a WebElement.

        :param web_element: The target element.
        :param expected_condition: Wait condition (see ``wait_for_element_we``).
        :param wait_time: Seconds to wait.
        :return: The element's visible text.
        """
        return self.wait_for_element_we(web_element, expected_condition, wait_time).text

    def get_value_we(
        self,
        web_element: WebElement,
        expected_condition: str | None = None,
        wait_time: float | None = 0,
    ) -> str:
        """
        Get the ``value`` attribute of a WebElement.

        :param web_element: The target element.
        :param expected_condition: Wait condition (see ``wait_for_element_we``).
        :param wait_time: Seconds to wait.
        :return: The element's ``value`` attribute.
        """
        return self.wait_for_element_we(
            web_element, expected_condition, wait_time
        ).get_attribute("value")

    def get_table_we(
        self,
        web_element: WebElement,
        expected_condition: str | None = None,
        wait_time: float | None = 0,
    ) -> pd.DataFrame:
        """
        Parse an HTML ``<table>`` WebElement into a DataFrame.

        :param web_element: The table element.
        :param expected_condition: Wait condition (see ``wait_for_element_we``).
        :param wait_time: Seconds to wait.
        :return: The table data.
        """
        element = self.wait_for_element_we(web_element, expected_condition, wait_time)
        return pd.read_html(StringIO(element.get_attribute("outerHTML")))[0]

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def press_button_we(
        self,
        web_element: WebElement,
        expected_condition: str | None = None,
        wait_time: float | None = 0,
    ) -> None:
        """
        Click a WebElement.

        :param web_element: The element to click.
        :param expected_condition: Wait condition (see ``wait_for_element_we``).
        :param wait_time: Seconds to wait.
        """
        self.wait_for_element_we(web_element, expected_condition, wait_time).click()

    def enter_text_we(
        self,
        text: str,
        web_element: WebElement,
        expected_condition: str | None = None,
        wait_time: float | None = 0,
    ) -> None:
        """
        Clear and populate a text field via WebElement.

        :param text: The text to enter.
        :param web_element: The input element.
        :param expected_condition: Wait condition (see ``wait_for_element_we``).
        :param wait_time: Seconds to wait.
        """
        element = self.wait_for_element_we(web_element, expected_condition, wait_time)
        element.clear()
        element.send_keys(text)

    def set_checkbox_state_we(
        self,
        state: bool,
        web_element: WebElement,
        expected_condition: str | None = None,
        wait_time: float | None = 0,
    ) -> None:
        """
        Set a checkbox to the desired state via WebElement.

        :param state: ``True`` to check, ``False`` to uncheck.
        :param web_element: The checkbox element.
        :param expected_condition: Wait condition (see ``wait_for_element_we``).
        :param wait_time: Seconds to wait.
        """
        element = self.wait_for_element_we(web_element, expected_condition, wait_time)
        if element.is_selected() != state:
            element.click()

    def set_select_option_we(
        self,
        option: str,
        web_element: WebElement,
        select_type: str | None = None,
        expected_condition: str | None = None,
        wait_time: float | None = 0,
    ) -> None:
        """
        Choose an option from a ``<select>`` dropdown via WebElement.

        :param option: The option to select (text, index, or value depending on
            *select_type*).
        :param web_element: The ``<select>`` element.
        :param select_type: Selection strategy: ``'value'`` (default), ``'index'``, or
            ``'visible_text'``.
        :param expected_condition: Wait condition (see ``wait_for_element_we``).
        :param wait_time: Seconds to wait.
        """
        element = self.wait_for_element_we(web_element, expected_condition, wait_time)
        select = Select(element)
        match select_type:
            case "index":
                select.select_by_index(int(option))
            case "visible_text":
                select.select_by_visible_text(option)
            case _:
                select.select_by_value(option)

    # ------------------------------------------------------------------
    # Presence / waiting
    # ------------------------------------------------------------------

    def web_page_contains_we(
        self,
        web_element: WebElement,
        expected_condition: str | None = None,
        wait_time: float | None = 0,
    ) -> WebElement | bool:
        """
        Check whether a WebElement is present and meets a condition.

        :param web_element: The element to check.
        :param expected_condition: Wait condition (see ``wait_for_element_we``).
        :param wait_time: Seconds to wait.
        :return: The element if found, otherwise ``False``.
        """
        try:
            return self.wait_for_element_we(
                web_element,
                expected_condition,
                self._get_wait_time(wait_time),
            )
        except (TimeoutException, NoSuchElementException):
            return False

    def text_is_present_we(
        self,
        web_element: WebElement,
        text: str,
        text_location: str | None = None,
        wait_time: float | None = 0,
    ) -> WebElement | bool:
        """
        Check whether *text* appears within a WebElement.

        :param web_element: The element to inspect.
        :param text: The text to search for.
        :param text_location: Where to look: ``'anywhere'`` (default), ``'attribute'``,
            or ``'value'``.
        :param wait_time: Seconds to wait.
        :return: The element if the text is found, otherwise ``False``.
        """
        match text_location:
            case "attribute":
                condition = EC.text_to_be_present_in_element_attribute
            case "value":
                condition = EC.text_to_be_present_in_element_value
            case _:
                condition = EC.text_to_be_present_in_element

        try:
            return WebDriverWait(self.driver, self._get_wait_time(wait_time)).until(
                condition(web_element, text)
            )
        except TimeoutException:
            return False
