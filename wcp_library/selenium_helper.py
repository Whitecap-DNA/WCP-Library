from pathlib import Path
from typing import Optional

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.remote.webelement import WebElement
from webdriver_manager.chrome import ChromeDriverManager


class SeleniumHelper:
    def __init__(self, headless: bool = False, download_path: Optional[Path] = None):
        self._headless = headless
        self._download_path = download_path

        opt = webdriver.ChromeOptions()
        opt.add_argument("--start-maximized")
        if headless:
            opt.add_argument('headless')

        opt.page_load_strategy = 'eager'

        experimental_options_dict = {"download.prompt_for_download": False,
                                        "download.directory_upgrade": True,
                                        "safebrowsing.enabled": True}
        if download_path:
            experimental_options_dict["download.default_directory"] = str(download_path)

        opt.add_experimental_option("prefs", experimental_options_dict)
        opt.timeouts = {'implicit': 5000}

        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opt)


    def find_button_by_text(self, text: str) -> WebElement:
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

    def find_span_by_text(self, text: str) -> WebElement:
        """
        Find a span by its text

        :param text:
        :return span element:
        """

        buttons = self.driver.find_elements(By.TAG_NAME, 'Span')
        for b in buttons:
            if b.text == text:
                return b

    def wait_until_element_visible(self, css_element: str, timeout: int = 30) -> None:
        """
        Wait until an element is visible

        :param css_element:
        :param timeout:
        :return:
        """

        WebDriverWait(self.driver, timeout).until(expected_conditions.presence_of_element_located((By.CSS_SELECTOR, css_element)))
