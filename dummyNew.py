from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium import webdriver
from time import sleep

try:
	options = Options() 
	options.set_headless(headless=True)
	
	driver = webdriver.Firefox(firefox_options=options)
	driver.implicitly_wait(10) 
	wait = WebDriverWait(driver, 10)
	driver.get("https://www.reddit.com/r/news/new/")
	actions = ActionChains(driver)

	for i in range(1,200):
		container_xpath = f'''//*[@id="SHORTCUT_FOCUSABLE_DIV"]/div/div[2]/div/div/div/div/div[2]/div[3]/div[1]/div/div[1]/div[{i}]'''
		element = wait.until( EC.presence_of_element_located( (By.XPATH, container_xpath)))
		n = element.find_elements_by_tag_name("h2")
		
		#Move so next ones load
		# actions.move_to_element(element).perform()
		driver.execute_script("arguments[0].scrollIntoView();", element)

		#Parse everything
		print(n[0].text)
		




finally:
	sleep(5)
	driver.quit()