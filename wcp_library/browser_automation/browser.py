"""
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
    with Browser(Firefox) as browser:
        browser.go_to("https://example.com")
"""

import logging
from typing import Dict, Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.edge.options import Options as EdgeOptions
from yarl import URL

from wcp_library.browser_automation.interactions import UIInteractions, WEInteractions

logger = logging.getLogger(__name__)

# +--------------------------------------------------------------------------------------------------------------------------------------------------------+
# |                                                    ===  Browser options and usage  ===                                                                 |
# +--------------+--------------------------------------------+-----------------------------------------------+--------------------------------------------+
# | Browser      | Description                                | JSON Configuration                            | Possible Permutations                      |
# +--------------+--------------------------------------------+-----------------------------------------------+--------------------------------------------+
# | All Browsers | Set browser timeouts (in ms)               | {"timeouts": {"implicit": 5000, ...}}         | implicit, pageLoad, script                 |
# | All Browsers | Name of the browser (e.g., 'chrome', ...)  | {"browserName": "chrome"}                     | chrome, firefox, edge, safari              |
# | All Browsers | Specific version of the browser to use.    | {"browserVersion": "latest"}                  | latest, 91.0, 90.0                         |
# | All Browsers | OS platform (e.g., 'Windows 10', 'Linux')  | {"platformName": "Windows 10"}                | Windows 10, Linux, macOS                   |
# | All Browsers | Strategy for page loads: normal, eager...  | {"pageLoadStrategy": "normal"}                | normal, eager, none                        |
# | All Browsers | Accept self-signed or invalid certs        | {"acceptInsecureCerts": true}                 | true, false                                |
# | Chrome       | Run browser in headless mode               | {"args": ["--headless"]}                      | --headless                                 |
# | Chrome       | Disable GPU acceleration                   | {"args": ["--disable-gpu"]}                   | --disable-gpu                              |
# | Chrome       | Set experimental options                   | {"prefs": {"download.default_directory":...}} | profile.default_content_settings.popups... |
# | Chrome       | Set path to Chrome binary                  | {"binary": "/path/to/chrome"}                 | /path/to/chrome                            |
# | Chrome       | Set Chrome extensions                      | {"extensions": ["/path/to/extension"]}        | /path/to/extension                         |
# | Chrome       | Exclude switches                           | {"excludeSwitches": ["enable-automation"]}    | enable-automation                          |
# | Chrome       | Use automation extension                   | {"useAutomationExtension": false}             | true, false                                |
# | Firefox      | Set download folder list                   | {"prefs": {"browser.download.folderList": 2}} | 1(Download folder), 2(User set directory)  |
# | Firefox      | Set download directory                     | {"prefs": {"browser.download.dir": "/tmp"}}   | /tmp                                       |
# | Firefox      | Run Firefox in headless mode               | {"args": ["-headless"]}                       | -headless                                  |
# | Firefox      | Set Firefox log level                      | {"log": {"level": "trace"}}                   | trace, debug, info, warn, error            |
# | Firefox      | Set Firefox profile                        | {"profile": "/path/to/profile"}               | /path/to/profile                           |
# | Firefox      | Set path to Firefox binary                 | {"binary": "/path/to/firefox"}                | /path/to/firefox                           |
# | Edge         | Run Edge in headless mode                  | {"args": ["--headless"]}                      | --headless                                 |
# | Edge         | Set path to Edge binary                    | {"binary": "/path/to/edge"}                   | /path/to/edge                              |
# | Edge         | Use Chromium-based Edge                    | {"useChromium": true}                         | true, false                                |
# | Edge         | Set Edge Chromium driver                   | {"edgeChromiumDriver": "/path/to/driver"}     | /path/to/driver                            |
# | Chrome/Edge  | Set initial window size                    | {"args": ["--window-size=1920,1080"]}         | --window-size=int,int                      |
# | Firefox      | Launch in private browsing mode            | {"args": ["-private"]}                        | -private                                   |
# +--------------+--------------------------------------------+-----------------------------------------------+--------------------------------------------+


class BaseSelenium(UIInteractions, WEInteractions):
    """
    Base class for Selenium-based browser automation.

    This class provides common functionality for initializing and managing Selenium
    WebDriver instances, as well as adding custom options to the WebDriver.

    :param browser_options: Dictionary containing custom options for the WebDriver.
    :param driver: Selenium WebDriver instance (optional, defaults to None).
    """

    def __init__(self, browser_options: dict = None):
        self.browser_options = browser_options or {}
        self.driver = None

    def __enter__(self) -> "BaseSelenium":
        self.driver = self._create_driver()
        super().__init__(self.driver)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type:
            logger.error(f"Exception occurred: {exc_type.__name__}: {exc_val}\nTraceback: {exc_tb}")
        if self.driver:
            self.driver.quit()

    def _create_driver(self) -> webdriver:
        """
        Abstract method to create a Selenium WebDriver instance.

        This method must be implemented by subclasses to instantiate and return
        a specific browser WebDriver (e.g., Chrome, Firefox, Edge).

        :return: Selenium WebDriver instance for the specified browser.
        :raises: NotImplementedError
        """

        raise NotImplementedError("Subclasses must implement this method")

    def _add_options(
        self, options: ChromeOptions | FirefoxOptions | EdgeOptions
    ) -> None:
        """
        Add custom options to the Selenium WebDriver.

        This method applies custom options such as headless mode, download paths,
        and command-line arguments to the WebDriver options.

        :param options: ChromeOptions | FirefoxOptions | EdgeOptions
        """

        if not self.browser_options:
            return

        # Apply standard Selenium options
        for key, value in self.browser_options.items():
            if hasattr(options, key) and "args" not in key:
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

    def go_to(self, url: str | URL):
        """Navigate to the specified URL.

        :param url: The URL to navigate to.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        if self.driver:
            self.driver.get(str(url))
        else:
            raise RuntimeError("WebDriver is not initialized.")

    def get_url(self) -> str:
        """Get the current URL of the page.

        :return: The current URL of the page.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        if self.driver:
            return self.driver.current_url
        raise RuntimeError("WebDriver is not initialized.")

    def get_title(self) -> str:
        """Get the title of the current page.

        :return: The title of the current page.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        if self.driver:
            return self.driver.title
        raise RuntimeError("WebDriver is not initialized.")

    def switch_to_window(
        self, window_handle: Optional[str] = None
    ) -> Optional[Dict[str, list]]:
        """
        Switches the browser context to a new window.

        If a specific window handle is provided, the driver will switch to that window.
        Otherwise, it will attempt to switch to a newly opened window that is different
        from the current one.

        :param window_handle: The handle of the window to switch to. If None, the method will search for a new window handle.
        :return: A dictionary containing the original window handle, the new window handle, and a list of all window handles at the time of switching.
        :raises RuntimeError: If the WebDriver is not initialized.
        """

        if window_handle:
            self.driver.switch_to.window(window_handle)
            return None

        original_window = self.driver.current_window_handle
        all_windows = self.driver.window_handles

        for new_window in all_windows:
            if new_window != original_window:
                self.driver.switch_to.window(new_window)
                return {
                    "original_window": original_window,
                    "new_window": new_window,
                    "all_windows": all_windows,
                }
        return None

    def close_window(self, window_handle: Optional[str] = None) -> None:
        """
        Closes the specified browser window.

        If a window handle is provided, that window will be closed.
        Otherwise, the currently active window will be closed.

        :param window_handle: The handle of the window to close. If None, the current window will be closed.
        """

        self.driver.close(window_handle or self.driver.current_window_handle)


class Firefox(BaseSelenium):
    """
    Class for Firefox browser automation using Selenium.

    This class extends the BaseSelenium class and provides functionality for creating
    and managing Firefox WebDriver instances.
    """

    def create_driver(self) -> webdriver.Firefox:
        options = FirefoxOptions()
        self._add_options(options)
        return webdriver.Firefox(options=options)


class Edge(BaseSelenium):
    """
    Class for Edge browser automation using Selenium.

    This class extends the BaseSelenium class and provides functionality for creating
    and managing Edge WebDriver instances.
    """

    def create_driver(self) -> webdriver.Edge:
        options = EdgeOptions()
        self._add_options(options)
        return webdriver.Edge(options=options)


class Chrome(BaseSelenium):
    """
    Class for Chrome browser automation using Selenium.

    This class extends the BaseSelenium class and provides functionality for creating
    and managing Chrome WebDriver instances.
    """

    def create_driver(self) -> webdriver.Chrome:
        options = ChromeOptions()
        self._add_options(options)
        return webdriver.Chrome(options=options)


class Browser(BaseSelenium):
    """
    Class for managing browser automation using Selenium.

    This class provides functionality for initializing and managing browser instances
    using the specified browser class and options.

    It acts as a context manager, allowing for easy setup and teardown of browser sessions.

    :param browser_class: The class of the browser to be used (e.g., Firefox, Edge, Chrome).
    :param browser_options: Dictionary containing custom options for the browser.
    """

    def __init__(self, browser_class: type, browser_options: dict = None):
        super().__init__(browser_options)
        self.browser_class = browser_class
        self.browser_options = browser_options or {}
        self.browser_instance = None

    def __enter__(self) -> BaseSelenium:
        self.browser_instance = self.browser_class(self.browser_options)
        self.browser_instance.driver = self.browser_instance.create_driver()
        return self.browser_instance

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type:
            logger.error(f"Exception occurred: {exc_type.__name__}: {exc_val}\nTraceback: {exc_tb}")
        if self.browser_instance and self.browser_instance.driver:
            self.browser_instance.driver.quit()
