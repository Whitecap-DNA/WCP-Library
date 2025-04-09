from pathlib import Path
from typing import Optional

from get_gecko_driver import GetGeckoDriver
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from wcp_library.selenium._selenium_driver import SeleniumDriver


class ChromeSeleniumHelper(SeleniumDriver):
    def __init__(self, headless: bool = False, download_path: Optional[Path] = None):
        super().__init__(headless, download_path)

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


class FirefoxSeleniumHelper(SeleniumDriver):
    def __init__(self, headless: bool = False, download_path: Optional[Path] = None):
        super().__init__(headless, download_path)

        opt = webdriver.FirefoxOptions()
        opt.add_argument("--start-maximized")
        if headless:
            opt.add_argument('--headless')

        opt.page_load_strategy = 'eager'

        if download_path:
            opt.set_preference("browser.download.folderList", 2)
            opt.set_preference("browser.download.dir", str(download_path))
            opt.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/octet-stream")

        get_driver = GetGeckoDriver()
        get_driver.install()
        self.driver = webdriver.Firefox(options=opt)