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
import re
import time
from typing import Any

import selenium.common.exceptions as selenium_exceptions
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from tenacity import retry as tenacity_retry
from yarl import URL

from wcp_library.browser_automation.interactions import (UIInteractions,
                                                         WEInteractions)
from wcp_library.retry import make_generic_retry

logger = logging.getLogger(__name__)


_SERVER_ERROR_PATTERN = re.compile(
    r"\b5\d{2}\b.{0,40}?\b(error|gateway|unavailable|timeout)\b",
    re.IGNORECASE | re.DOTALL,
)


class BaseSelenium(UIInteractions, WEInteractions):
    """
    Abstract base class for Selenium-based browser automation.

    Inherits element-interaction capabilities from ``UIInteractions`` and
    ``WEInteractions`` and adds browser lifecycle methods (navigation,
    window management, JavaScript execution, etc.).

    :param browser_options: Custom WebDriver options (headless mode, arguments, download
        path, timeouts, etc.).
    :param sharepoint_config: Configuration for uploading error screenshots to
        SharePoint. Expected keys: ``site_id``, ``app_id``, ``app_secret``,
        ``tenant_id``
    :ivar driver: The active WebDriver instance, set after entering the context manager.
    :ivar browser_options: Resolved browser options.
    :ivar sharepoint_config: SharePoint configuration passed to the ``Interactions``
        base.
    """

    class SeleniumExceptions:
        """
        Container for all Selenium exception classes.

        :ivar ALL: Every ``Exception`` subclass defined in
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
        """Initialize the BaseSelenium instance.
        Browser options reference: https://github.com/Whitecap-DNA/WCP-Library/wiki/Helper-%E2%80%90-Browser-Automation#available-browser-options
        SharePoint configuration reference: https://github.com/Whitecap-DNA/WCP-Library/wiki/Helper-%E2%80%90-Browser-Automation#sharepoint-configuration-optional
        """
        self.browser_options = browser_options or {}
        self.sharepoint_config = sharepoint_config
        self.driver = None
        # Initialise the Interactions base with a None driver; the real
        # driver is injected in __enter__ once create_driver() succeeds.
        super().__init__(driver=None, sharepoint_config=sharepoint_config)

    @tenacity_retry(
        **make_generic_retry(exceptions=(selenium_exceptions.WebDriverException,))
    )
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

        :return: A newly created WebDriver instance.
        :raises NotImplementedError: Always, unless overridden by a subclass.
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

        :param options: The browser options instance to configure.
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

        :param url: The target URL.
        :raises RuntimeError: If the WebDriver is not initialised.
        """
        if self.driver:
            self.driver.get(str(url))
        else:
            raise RuntimeError("WebDriver is not initialized.")

    def refresh_page(self) -> None:
        """
        Refresh the current page.

        :raises RuntimeError: If the WebDriver is not initialised.
        """
        if self.driver:
            self.driver.refresh()
        else:
            raise RuntimeError("WebDriver is not initialized.")

    def get_url(self) -> str:
        """
        Return the current page URL.

        :return: The current URL.
        :raises RuntimeError: If the WebDriver is not initialised.
        """
        if self.driver:
            return self.driver.current_url
        raise RuntimeError("WebDriver is not initialized.")

    def get_title(self) -> str:
        """
        Return the current page title.

        :return: The page title.
        :raises RuntimeError: If the WebDriver is not initialised.
        """
        if self.driver:
            return self.driver.title
        raise RuntimeError("WebDriver is not initialized.")

    def is_server_error_page(self) -> bool:
        """
        Check whether the current page is a 5xx server/gateway error page.

        Looks for common patterns such as '502 Bad Gateway', '503 Service
        Unavailable', '504 Gateway Timeout', or '500 Internal Server Error'
        in the page title and page source.

        :return: True if a 5xx error pattern is found. False otherwise, including when
            the page cannot be read at all.
        """
        if self.driver:

            try:
                title = self.driver.title or ""
                page_source = self.driver.page_source or ""
            except Exception:
                return False

            haystack = f"{title}\n{page_source}"
            return bool(_SERVER_ERROR_PATTERN.search(haystack))
        return False

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

        :param window_handle: Explicit handle to switch to. If ``None``, the first
            window that is not the current one is used.
        :return: A dictionary with keys ``'original_window'``, ``'new_window'``, and
            ``'all_windows'`` when a new window was found, or ``None`` if
            *window_handle* was given or no new window exists.
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

        :param window_handle: Handle of the window to close. If ``None``, the current
            window is closed.
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

        :param wait_time: Seconds to sleep.
        """
        time.sleep(wait_time)

    def execute_script(self, script: str, *args) -> Any:
        """
        Execute JavaScript in the browser context.

        :param script: JavaScript source code.
        :param args: Arguments passed to the script (accessible as ``arguments[0]``,
            ``arguments[1]``, etc.).
        :return: The value returned by the script.
        :raises RuntimeError: If the WebDriver is not initialised.
        :raises WebDriverException: If script execution fails.
        """
        if self.driver:
            return self.driver.execute_script(script, *args)
        raise RuntimeError("WebDriver is not initialized.")


class Browser:
    """
    Context manager for browser session lifecycle.

    Wraps a browser subclass (``Browser.Firefox``, ``Browser.Chrome``, or
    ``Browser.Edge``) and manages driver creation and teardown.

    :param browser_class: The browser subclass to instantiate (e.g.
        ``Browser.Firefox``).
    :param browser_options: Custom WebDriver options forwarded to the browser subclass.
    :param sharepoint_config: Configuration for uploading error screenshots to
        SharePoint.
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

        :param browser_options: Custom options forwarded to ``FirefoxOptions``.
        """

        def create_driver(self) -> webdriver.Firefox:
            """
            Create a Firefox WebDriver instance.

            :return: A configured Firefox driver.
            """
            options = FirefoxOptions()
            self._add_options(options)
            return webdriver.Firefox(options=options)

    class Edge(BaseSelenium):
        """
        Edge WebDriver implementation.

        :param browser_options: Custom options forwarded to ``EdgeOptions``.
        """

        def create_driver(self) -> webdriver.Edge:
            """
            Create an Edge WebDriver instance.

            :return: A configured Edge driver.
            """
            options = EdgeOptions()
            self._add_options(options)
            return webdriver.Edge(options=options)

    class Chrome(BaseSelenium):
        """
        Chrome WebDriver implementation.

        :param browser_options: Custom options forwarded to ``ChromeOptions``.
        """

        def create_driver(self) -> webdriver.Chrome:
            """
            Create a Chrome WebDriver instance.

            :return: A configured Chrome driver.
            """
            options = ChromeOptions()
            self._add_options(options)
            return webdriver.Chrome(options=options)
