import os

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service


class SeleniumObj:
    """
    Class for selenium objects
    """

    def __init__(self, headless=True, show_display=False, incognito=False, cache=False):
        self.download_path = os.getenv("DOWNLOAD_FOLDER")
        self.webdriver_path = os.getenv("WEBDRIVER_PATH")

        options = Options()

        # option set to avoid 'DevToolsActivePort file doesn't exist' error
        options.add_argument("--no-sandbox")

        if headless:
            options.add_argument("--headless")
            # pass

        if not os.path.isdir(self.download_path):
            os.makedirs(self.download_path)

        if self.download_path is not None:
            prefs = dict()
            prefs["profile.default_content_settings.popups"] = 0
            prefs["download.default_directory"] = self.download_path
            options.add_experimental_option("prefs", prefs)

        if incognito:
            options.add_argument("--incognito")

        if not cache:
            options.add_argument("--disable-cache")

        service = Service(executable_path=self.webdriver_path)
        self.driver = webdriver.Chrome(service=service, options=options)