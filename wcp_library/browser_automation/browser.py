"""
Browser automation framework using Selenium WebDriver.

Provides a base class for browser setup, option configuration, and lifecycle
management, with concrete subclasses for Chrome, Firefox, and Edge. The
``Browser`` context manager simplifies session creation and teardown.

Classes
-------
BaseSelenium
    Abstract base class encapsulating shared WebDriver functionality.
Browser
    Context manager that pairs a browser subclass with its options.
Browser.Chrome
    Chrome-specific WebDriver implementation.
Browser.Firefox
    Firefox-specific WebDriver implementation.
Browser.Edge
    Edge-specific WebDriver implementation.

Usage
--------
browser_options = {
    "args": ["--headless", "--disable-gpu"],
    "timeouts": {"pageLoad": 30000, "implicit": 5000},
}

config = {
    "headers": {"Authorization": f"Bearer {token}"},
    "site_id": "site-id",
}

with Browser(Browser.Firefox, browser_options=browser_options, sharepoint_config=config) as browser:
    browser.go_to("https://example.com")

"""

import inspect
import logging
import time
from typing import Any

import selenium.common.exceptions as selenium_exceptions
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from yarl import URL

from wcp_library import retry
from wcp_library.browser_automation.interactions import (UIInteractions,
                                                         WEInteractions)

logger = logging.getLogger(__name__)


class BaseSelenium(UIInteractions, WEInteractions):
    """
    Abstract base class for Selenium-based browser automation.

    Inherits element-interaction capabilities from ``UIInteractions`` and
    ``WEInteractions`` and adds browser lifecycle methods (navigation,
    window management, JavaScript execution, etc.).

    Parameters
    ----------
    browser_options : dict or None, optional
        Custom WebDriver options (headless mode, arguments, download path,
        timeouts, etc.).
    sharepoint_config : dict or None, optional
        Configuration for uploading error screenshots to SharePoint.
        Expected keys: ``headers``, ``site_id``, ``file_path``.

    Attributes
    ----------
    driver : selenium.webdriver.remote.webdriver.WebDriver or None
        The active WebDriver instance, set after entering the context manager.
    browser_options : dict
        Resolved browser options.
    sharepoint_config : dict or None
        SharePoint configuration passed to the ``Interactions`` base.
    """

    class SeleniumExceptions:
        """
        Container for all Selenium exception classes.

        Attributes
        ----------
        ALL : tuple of type
            Every ``Exception`` subclass defined in
            ``selenium.common.exceptions``.
        """

        ALL: tuple[type, ...] = tuple(
            obj
            for _, obj in inspect.getmembers(selenium_exceptions)
            if inspect.isclass(obj) and issubclass(obj, Exception)
        )

    def __init__(
        self,
        browser_options: dict | None = None,
        sharepoint_config: dict | None = None,
    ) -> None:
        self.browser_options = browser_options or {}
        self.sharepoint_config = sharepoint_config
        self.driver = None
        # Initialise the Interactions base with a None driver; the real
        # driver is injected in __enter__ once create_driver() succeeds.
        super().__init__(driver=None, sharepoint_config=sharepoint_config)

    @retry(exceptions=(selenium_exceptions.WebDriverException,))
    def __enter__(self) -> "BaseSelenium":
        self.driver = self.create_driver()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type:
            logger.error(
                "Exception occurred: %s: %s\nTraceback: %s",
                exc_type.__name__ if exc_type else None,
                exc_val,
                exc_tb,
            )
        if self.driver:
            self.driver.quit()

    # ------------------------------------------------------------------
    # Driver creation (abstract)
    # ------------------------------------------------------------------

    def create_driver(self) -> webdriver.Remote:
        """
        Create a Selenium WebDriver instance.

        Subclasses **must** override this method to return a browser-specific
        driver (Chrome, Firefox, Edge, etc.).

        Returns
        -------
        selenium.webdriver.remote.webdriver.WebDriver
            A newly created WebDriver instance.

        Raises
        ------
        NotImplementedError
            Always, unless overridden by a subclass.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    # ------------------------------------------------------------------
    # Option configuration
    # ------------------------------------------------------------------

    def _add_options(
        self,
        options: ChromeOptions | FirefoxOptions | EdgeOptions,
    ) -> None:
        """
        Apply custom options to a browser ``Options`` object.

        Handles standard Selenium attributes, command-line arguments, and
        download-path configuration for each supported browser family.

        Parameters
        ----------
        options : ChromeOptions, FirefoxOptions, or EdgeOptions
            The browser options instance to configure.
        """
        if not self.browser_options:
            return

        # Standard Selenium attributes
        for key, value in self.browser_options.items():
            if hasattr(options, key) and "args" not in key:
                setattr(options, key, value)

        # Command-line arguments
        for arg in self.browser_options.get("args", []):
            options.add_argument(arg)

        # Download path
        download_path = self.browser_options.get("download_path")
        if download_path:
            if isinstance(options, FirefoxOptions):
                options.set_preference("browser.download.folderList", 2)
                options.set_preference("browser.download.dir", str(download_path))
                options.set_preference(
                    "browser.helperApps.neverAsk.saveToDisk",
                    "application/octet-stream",
                )
            elif isinstance(options, (ChromeOptions, EdgeOptions)):
                options.add_experimental_option(
                    "prefs",
                    {
                        "download.default_directory": str(download_path),
                        "download.prompt_for_download": False,
                        "directory_upgrade": True,
                    },
                )

    # ------------------------------------------------------------------
    # Navigation
    # ------------------------------------------------------------------

    def go_to(self, url: str | URL) -> None:
        """
        Navigate to the specified URL.

        Parameters
        ----------
        url : str or yarl.URL
            The target URL.

        Raises
        ------
        RuntimeError
            If the WebDriver is not initialised.
        """
        if self.driver:
            self.driver.get(str(url))
        else:
            raise RuntimeError("WebDriver is not initialized.")

    def refresh_page(self) -> None:
        """
        Refresh the current page.

        Raises
        ------
        RuntimeError
            If the WebDriver is not initialised.
        """
        if self.driver:
            self.driver.refresh()
        else:
            raise RuntimeError("WebDriver is not initialized.")

    def get_url(self) -> str:
        """
        Return the current page URL.

        Returns
        -------
        str
            The current URL.

        Raises
        ------
        RuntimeError
            If the WebDriver is not initialised.
        """
        if self.driver:
            return self.driver.current_url
        raise RuntimeError("WebDriver is not initialized.")

    def get_title(self) -> str:
        """
        Return the current page title.

        Returns
        -------
        str
            The page title.

        Raises
        ------
        RuntimeError
            If the WebDriver is not initialised.
        """
        if self.driver:
            return self.driver.title
        raise RuntimeError("WebDriver is not initialized.")

    # ------------------------------------------------------------------
    # Window management
    # ------------------------------------------------------------------

    def switch_to_window(
        self,
        window_handle: str | list | None = None,
    ) -> dict[str, str | list] | None:
        """
        Switch the browser context to another window.

        When *window_handle* is provided the driver switches directly.
        Otherwise the method searches for a newly opened window that differs
        from the current one.

        Parameters
        ----------
        window_handle : str, list, or None, optional
            Explicit handle to switch to. If ``None``, the first window
            that is not the current one is used.

        Returns
        -------
        dict or None
            A dictionary with keys ``'original_window'``, ``'new_window'``,
            and ``'all_windows'`` when a new window was found, or ``None``
            if *window_handle* was given or no new window exists.
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

        self.force_wait(1)
        return None

    def close_window(self, window_handle: str | None = None) -> None:
        """
        Close a browser window.

        Parameters
        ----------
        window_handle : str or None, optional
            Handle of the window to close. If ``None``, the current window
            is closed.
        """
        if window_handle:
            current_window = self.driver.current_window_handle
            self.switch_to_window(current_window)
        self.driver.close()

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def force_wait(wait_time: int | float) -> None:
        """
        Block execution for a fixed duration.

        Parameters
        ----------
        wait_time : int or float
            Seconds to sleep.
        """
        time.sleep(wait_time)

    def execute_script(self, script: str, *args) -> Any:
        """
        Execute JavaScript in the browser context.

        Parameters
        ----------
        script : str
            JavaScript source code.
        *args
            Arguments passed to the script (accessible as
            ``arguments[0]``, ``arguments[1]``, etc.).

        Returns
        -------
        Any
            The value returned by the script.

        Raises
        ------
        RuntimeError
            If the WebDriver is not initialised.
        WebDriverException
            If script execution fails.
        """
        if self.driver:
            return self.driver.execute_script(script, *args)
        raise RuntimeError("WebDriver is not initialized.")


class Browser:
    """
    Context manager for browser session lifecycle.

    Wraps a browser subclass (``Browser.Firefox``, ``Browser.Chrome``, or
    ``Browser.Edge``) and manages driver creation and teardown.

    Parameters
    ----------
    browser_class : type
        The browser subclass to instantiate (e.g. ``Browser.Firefox``).
    browser_options : dict or None, optional
        Custom WebDriver options forwarded to the browser subclass.
    sharepoint_config : dict or None, optional
        Configuration for uploading error screenshots to SharePoint.
    """

    SeleniumExceptions = BaseSelenium.SeleniumExceptions

    def __init__(
        self,
        browser_class: type,
        browser_options: dict | None = None,
        sharepoint_config: dict | None = None,
    ) -> None:
        self.browser_class = browser_class
        self.browser_options = browser_options or {}
        self.sharepoint_config = sharepoint_config
        self.browser_instance: BaseSelenium | None = None

    def __enter__(self) -> BaseSelenium:
        self.browser_instance = self.browser_class(
            self.browser_options, self.sharepoint_config
        )
        self.browser_instance.driver = self.browser_instance.create_driver()
        return self.browser_instance

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type:
            logger.error(
                "Exception occurred: %s: %s\nTraceback: %s",
                exc_type.__name__ if exc_type else None,
                exc_val,
                exc_tb,
            )
        if self.browser_instance and self.browser_instance.driver:
            self.browser_instance.driver.quit()

    # ------------------------------------------------------------------
    # Browser subclasses
    # ------------------------------------------------------------------

    class Firefox(BaseSelenium):
        """
        Firefox WebDriver implementation.

        Parameters
        ----------
        browser_options : dict or None, optional
            Custom options forwarded to ``FirefoxOptions``.
        """

        def create_driver(self) -> webdriver.Firefox:
            """
            Create a Firefox WebDriver instance.

            Returns
            -------
            selenium.webdriver.Firefox
                A configured Firefox driver.
            """
            options = FirefoxOptions()
            self._add_options(options)
            return webdriver.Firefox(options=options)

    class Edge(BaseSelenium):
        """
        Edge WebDriver implementation.

        Parameters
        ----------
        browser_options : dict or None, optional
            Custom options forwarded to ``EdgeOptions``.
        """

        def create_driver(self) -> webdriver.Edge:
            """
            Create an Edge WebDriver instance.

            Returns
            -------
            selenium.webdriver.Edge
                A configured Edge driver.
            """
            options = EdgeOptions()
            self._add_options(options)
            return webdriver.Edge(options=options)

    class Chrome(BaseSelenium):
        """
        Chrome WebDriver implementation.

        Parameters
        ----------
        browser_options : dict or None, optional
            Custom options forwarded to ``ChromeOptions``.
        """

        def create_driver(self) -> webdriver.Chrome:
            """
            Create a Chrome WebDriver instance.

            Returns
            -------
            selenium.webdriver.Chrome
                A configured Chrome driver.
            """
            options = ChromeOptions()
            self._add_options(options)
            return webdriver.Chrome(options=options)
