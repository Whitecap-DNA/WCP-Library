"""
browser.py

This module provides a framework for browser automation using Selenium WebDriver.

It defines a base class `BaseSelenium` that encapsulates shared functionality for browser setup,
option configuration, and lifecycle management. Subclasses for specific browsers—`Chrome`,
`Firefox`, and `Edge`—extend this base to implement browser-specific driver creation.

Additionally, the `Browser` context manager simplifies the use of these classes by managing
initialization and cleanup of browser sessions.

Classes:
    BaseSelenium: Abstract base class for browser automation.
    Chrome: Chrome-specific WebDriver implementation.
    Firefox: Firefox-specific WebDriver implementation.
    Edge: Edge-specific WebDriver implementation.
    Browser: Context manager for browser session lifecycle.

Usage:
    with Browser(Firefox, {"explicit_wait": 5}) as browser:
        browser.go_to("https://example.com")
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.edge.options import Options as EdgeOptions

from wcp_library.browser_automation.interactions import UIInteractions, WEInteractions


class BaseSelenium(UIInteractions, WEInteractions):
    """
    Base class for Selenium-based browser automation.

    This class provides common functionality for initializing and managing Selenium
    WebDriver instances, as well as adding custom options to the WebDriver.

    Attributes:
        browser_options (dict): Dictionary containing custom options for the WebDriver.
        driver (webdriver): Selenium WebDriver instance.
    """

    def __init__(self, browser_options: dict = None):
        self.browser_options = browser_options or {}
        self.driver = None

    def __enter__(self) -> "BaseSelenium":
        self.driver = self.create_driver()
        super().__init__(self.driver)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type:
            print(
                f"Exception occurred: {exc_type.__name__}: {exc_val}\nTraceback: {exc_tb}"
            )
        if self.driver:
            self.driver.quit()

    def create_driver(self) -> webdriver:
        """
        Abstract method to create a Selenium WebDriver instance.

        This method must be implemented by subclasses to instantiate and return
        a specific browser WebDriver (e.g., Chrome, Firefox, Edge).

        Returns:
            webdriver: A Selenium WebDriver instance for the specified browser.

        Raises:
            NotImplementedError: If the method is not implemented in the subclass.
        """
        raise NotImplementedError("Subclasses must implement this method")

    def add_options(
        self, options: ChromeOptions | FirefoxOptions | EdgeOptions
    ) -> None:
        """
        Add custom options to the Selenium WebDriver.

        This method applies custom options such as headless mode, download paths,
        and command-line arguments to the WebDriver options.

        Args:
            options (ChromeOptions | FirefoxOptions | EdgeOptions): The WebDriver options to modify.
        """
        if not self.browser_options:
            return

        if self.browser_options.get("explicit_wait"):
            self.wait_time = self.browser_options["explicit_wait"]

        # Apply standard Selenium options
        for key, value in self.browser_options.items():
            if hasattr(options, key) and key != ("args" or "explicit_wait"):
                setattr(options, key, value)

        # Apply command-line arguments
        args = self.browser_options.get("args", [])
        for arg in args:
            options.add_argument(arg)

        # Handle download path
        download_path = self.browser_options.get("download_path")
        if download_path:
            if isinstance(options, FirefoxOptions):
                options.set_preference("browser.download.folderList", 2)
                options.set_preference("browser.download.dir", str(download_path))
                options.set_preference(
                    "browser.helperApps.neverAsk.saveToDisk", "application/octet-stream"
                )
            elif isinstance(options, (ChromeOptions, EdgeOptions)):
                prefs = {
                    "download.default_directory": str(download_path),
                    "download.prompt_for_download": False,
                    "directory_upgrade": True,
                }
                options.add_experimental_option("prefs", prefs)


class Firefox(BaseSelenium):
    """
    Class for Firefox browser automation using Selenium.

    This class extends the BaseSelenium class and provides functionality for creating
    and managing Firefox WebDriver instances.
    """

    def create_driver(self) -> webdriver.Firefox:
        options = FirefoxOptions()
        self.add_options(options)
        return webdriver.Firefox(options=options)


class Edge(BaseSelenium):
    """
    Class for Edge browser automation using Selenium.

    This class extends the BaseSelenium class and provides functionality for creating
    and managing Edge WebDriver instances.
    """

    def create_driver(self) -> webdriver.Edge:
        options = EdgeOptions()
        self.add_options(options)
        return webdriver.Edge(options=options)


class Chrome(BaseSelenium):
    """
    Class for Chrome browser automation using Selenium.

    This class extends the BaseSelenium class and provides functionality for creating
    and managing Chrome WebDriver instances.
    """

    def create_driver(self) -> webdriver.Chrome:
        options = ChromeOptions()
        self.add_options(options)
        return webdriver.Chrome(options=options)


class Browser:
    """
    Class for managing browser automation using Selenium.

    This class provides functionality for initializing and managing browser instances
    using the specified browser class and options.

    Attributes:
        browser_class (type): The class of the browser to be used (e.g., Firefox, Edge, Chrome).
        browser_options (dict): Dictionary containing custom options for the browser.
        browser_instance (BaseSelenium): Instance of the browser class.
    """

    def __init__(self, browser_class: type, browser_options: dict = None):
        self.browser_class = browser_class
        self.browser_options = browser_options or {}
        self.browser_instance = None

    def __enter__(self) -> BaseSelenium:
        self.browser_instance = self.browser_class(self.browser_options)
        self.browser_instance.driver = self.browser_instance.create_driver()
        return self.browser_instance

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type:
            print(
                f"Exception occurred: {exc_type.__name__}: {exc_val}\nTraceback: {exc_tb}"
            )
        if self.browser_instance and self.browser_instance.driver:
            self.browser_instance.driver.quit()
