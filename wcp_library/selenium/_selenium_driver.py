from abc import ABC
from dataclasses import field
from typing import Optional

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait


class SeleniumDriver(ABC):
    def __init__(self, headless: bool = True, download_path: str = None):
        self._headless = headless
        self._download_path = download_path
        self.driver = field(init=False)

    def find_button_by_text(self, text: str) -> WebElement | None:
        """
        Find a button by its text

        :param text:
        :return button element:
        """

        buttons = self.driver.find_elements(By.TAG_NAME, 'Button')
        for b in buttons:
            if b.text == text:
                return b

        # If there is something weird with the button text, this should find it
        for b in buttons:
            if text in b.text:
                return b

    def find_span_by_text(self, text: str) -> WebElement | None:
        """
        Find a span by its text

        :param text:
        :return span element:
        """

        buttons = self.driver.find_elements(By.TAG_NAME, 'Span')
        for b in buttons:
            if b.text == text:
                return b

    def wait_until_element_visible(self, css_element: Optional[str] = None, link_text: Optional[str] = None,
                                   timeout: int = 30) -> None:
        """
        Wait until an element is visible

        Must pass one of: css_element or link_text

        :param css_element:
        :param link_text:
        :param timeout:
        :return:
        """

        if not css_element and not link_text:
            raise ValueError("Must pass one of: css_element or link_text")

        if css_element:
            WebDriverWait(self.driver, timeout).until(expected_conditions.presence_of_element_located((By.CSS_SELECTOR, css_element)))
        elif link_text:
            WebDriverWait(self.driver, timeout).until(expected_conditions.presence_of_element_located((By.LINK_TEXT, link_text)))

    def wait_until_clickable(self, css_element: Optional[str] = None, link_text: Optional[str] = None,
                             timeout: int = 30) -> None:
        """
        Wait until an element is clickable

        Must pass one of: css_element or link_text

        :param css_element:
        :param link_text:
        :param timeout:
        :return:
        """

        if not css_element and not link_text:
            raise ValueError("Must pass one of: css_element or link_text")

        if css_element:
            WebDriverWait(self.driver, timeout).until(expected_conditions.element_to_be_clickable((By.CSS_SELECTOR, css_element)))
        elif link_text:
            WebDriverWait(self.driver, timeout).until(expected_conditions.element_to_be_clickable((By.LINK_TEXT, link_text)))