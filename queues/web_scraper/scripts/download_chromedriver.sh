#!/bin/bash
# Script to manually download and setup ChromeDriver that matches your Chrome version

# Determine Chrome version
CHROME_VERSION=$(google-chrome --version | awk '{print $3}' | cut -d '.' -f 1)
echo "Detected Chrome version: $CHROME_VERSION"

# Create directory for ChromeDriver
mkdir -p ~/chromedriver
cd ~/chromedriver

# Download appropriate ChromeDriver
echo "Downloading ChromeDriver for Chrome version $CHROME_VERSION"
wget -N "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_$CHROME_VERSION" -O latest_release
DRIVER_VERSION=$(cat latest_release)
echo "Latest ChromeDriver version for Chrome $CHROME_VERSION is: $DRIVER_VERSION"

# Download and extract the driver
wget -N "https://chromedriver.storage.googleapis.com/$DRIVER_VERSION/chromedriver_linux64.zip"
unzip -o chromedriver_linux64.zip

# Make it executable
chmod +x chromedriver

# Add to PATH
echo "ChromeDriver downloaded to: $(pwd)/chromedriver"
echo "To use this ChromeDriver, update your script to use the specific path:"
echo ""
echo "from selenium.webdriver.chrome.service import Service"
echo "driver = webdriver.Chrome(service=Service('$(pwd)/chromedriver'), options=opts)"
echo ""
echo "Or run the following to use this ChromeDriver for this session:"
echo "export PATH=$(pwd):$PATH"