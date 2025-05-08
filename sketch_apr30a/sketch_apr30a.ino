#include <WiFi.h>
#include <HTTPClient.h>
#include "esp_camera.h"
#include "DHT.h"

// WiFi credentials
const char* ssid = "Mana_Illu";
const char* password = "ManaIllu";

// FastAPI server endpoint
const char* serverName = "http://10.0.0.85:8000/upload";

// DHT settings
#define DHTPIN 13
#define DHTTYPE DHT11
DHT dht(DHTPIN, DHTTYPE);

// AI Thinker ESP32-CAM pin definitions
#define PWDN_GPIO_NUM     32
#define RESET_GPIO_NUM    -1
#define XCLK_GPIO_NUM      0
#define SIOD_GPIO_NUM     26
#define SIOC_GPIO_NUM     27
#define Y9_GPIO_NUM       35
#define Y8_GPIO_NUM       34
#define Y7_GPIO_NUM       39
#define Y6_GPIO_NUM       36
#define Y5_GPIO_NUM       21
#define Y4_GPIO_NUM       19
#define Y3_GPIO_NUM       18
#define Y2_GPIO_NUM        5
#define VSYNC_GPIO_NUM    25
#define HREF_GPIO_NUM     23
#define PCLK_GPIO_NUM     22

void connectToWiFi() {
  Serial.print("Connecting to WiFi");
  WiFi.begin(ssid, password);
  while (WiFi.status() != WL_CONNECTED) {
    Serial.print(".");
    delay(500);
  }
  Serial.println("\nWiFi connected");
  Serial.print("IP address: ");
  Serial.println(WiFi.localIP());
}

void setupCamera() {
  camera_config_t config;
  config.ledc_channel = LEDC_CHANNEL_0;
  config.ledc_timer = LEDC_TIMER_0;
  config.pin_d0 = Y2_GPIO_NUM;
  config.pin_d1 = Y3_GPIO_NUM;
  config.pin_d2 = Y4_GPIO_NUM;
  config.pin_d3 = Y5_GPIO_NUM;
  config.pin_d4 = Y6_GPIO_NUM;
  config.pin_d5 = Y7_GPIO_NUM;
  config.pin_d6 = Y8_GPIO_NUM;
  config.pin_d7 = Y9_GPIO_NUM;
  config.pin_xclk = XCLK_GPIO_NUM;
  config.pin_pclk = PCLK_GPIO_NUM;
  config.pin_vsync = VSYNC_GPIO_NUM;
  config.pin_href = HREF_GPIO_NUM;
  config.pin_sscb_sda = SIOD_GPIO_NUM;
  config.pin_sscb_scl = SIOC_GPIO_NUM;
  config.pin_pwdn = PWDN_GPIO_NUM;
  config.pin_reset = RESET_GPIO_NUM;
  config.xclk_freq_hz = 20000000;
  config.pixel_format = PIXFORMAT_JPEG;

  if (psramFound()) {
    config.frame_size = FRAMESIZE_VGA;
    config.jpeg_quality = 10;
    config.fb_count = 2;
  } else {
    config.frame_size = FRAMESIZE_QVGA;
    config.jpeg_quality = 12;
    config.fb_count = 1;
  }

  esp_err_t err = esp_camera_init(&config);
  if (err != ESP_OK) {
    Serial.printf("Camera init failed with error 0x%x\n", err);
    while (true);
  }
}

#include <WiFiClient.h>

void sendDataToServer(float temp, float hum, bool alert, camera_fb_t* fb) {
  WiFiClient client;

  if (!client.connect("10.0.0.85", 8000)) {
    Serial.println("Connection to server failed");
    return;
  }

  String boundary = "----WebKitFormBoundary7MA4YWxkTrZu0gW";
  String head = "--" + boundary + "\r\n";
  head += "Content-Disposition: form-data; name=\"temperature\"\r\n\r\n";
  head += String(temp) + "\r\n";
  head += "--" + boundary + "\r\n";
  head += "Content-Disposition: form-data; name=\"humidity\"\r\n\r\n";
  head += String(hum) + "\r\n";
  head += "--" + boundary + "\r\n";
  head += "Content-Disposition: form-data; name=\"alert\"\r\n\r\n";
  head += String(alert) + "\r\n";
  head += "--" + boundary + "\r\n";
  head += "Content-Disposition: form-data; name=\"image\"; filename=\"image.jpg\"\r\n";
  head += "Content-Type: image/jpeg\r\n\r\n";

  String tail = "\r\n--" + boundary + "--\r\n";

  int contentLength = head.length() + fb->len + tail.length();

  // Send HTTP POST request manually
  client.println("POST /upload HTTP/1.1");
  client.println("Host: 10.0.0.85");
  client.println("Content-Type: multipart/form-data; boundary=" + boundary);
  client.println("Content-Length: " + String(contentLength));
  client.println("Connection: close");
  client.println();
  client.print(head);

  // Send image buffer in chunks
  client.write(fb->buf, fb->len);

  client.print(tail);

  // Read response
  while (client.connected()) {
    String line = client.readStringUntil('\n');
    if (line == "\r") break;
  }

  String response = client.readString();
  Serial.println("Server response:");
  Serial.println(response);

  client.stop();
}

void setup() {
  Serial.begin(115200);
  delay(1000);
  Serial.println("Booting...");
  connectToWiFi();
  setupCamera();
  dht.begin();
}

void loop() {
  float temp = dht.readTemperature();
  float hum = dht.readHumidity();
  bool alert = temp > 25.0;

  Serial.println(alert ? "🌡️ ALERT: fire-threshold exceeded" : "OK: temp below threshold");

  if (isnan(temp) || isnan(hum)) {
    Serial.println("Failed to read from DHT");
    delay(10000);
    return;
  }

  camera_fb_t* fb = esp_camera_fb_get();
  if (!fb) {
    Serial.println("Camera capture failed");
    delay(10000);
    return;
  }

  sendDataToServer(temp, hum, alert, fb);
  esp_camera_fb_return(fb);

  delay(15000);
}