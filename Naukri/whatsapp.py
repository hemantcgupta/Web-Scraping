# -*- coding: utf-8 -*-
"""
Created on Sun Apr  2 14:38:20 2023

@author: Hemant
"""

from selenium import webdriver
import time

driver = webdriver.Chrome()

# open WhatsApp web
driver.get("https://web.whatsapp.com/")

# wait for the user to scan the QR code
input("Scan the QR code and press Enter when you're logged in: ")

# find the search input element and search for the contact
search_input = driver.find_element_by_xpath('//div[@contenteditable="true"][@data-tab="3"]')
contact_name = "Contact Name"
search_input.send_keys(contact_name)
time.sleep(2)
search_input.send_keys(Keys.RETURN)

# wait for the chat to load
time.sleep(5)

# find the input element and send the message
message_input = driver.find_element_by_xpath('//div[@contenteditable="true"][@data-tab="6"]')
message_text = "Hello, this message was sent using Python!"
message_input.send_keys(message_text)
message_input.send_keys(Keys.RETURN)

# close the browser
driver.quit()


import pywhatkit as kit
import datetime

# Get current hour and minute
now = datetime.datetime.now()
current_hour = now.hour
current_min = now.minute

# Send message
kit.sendwhatmsg("+919167296152", "Hello, this is a test message", current_hour, current_min + 1)
