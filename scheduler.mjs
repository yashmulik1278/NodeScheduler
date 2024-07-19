import 'dotenv/config';
import path from 'path';
import { fileURLToPath } from 'url';
import axios from 'axios';
import schedule from 'node-schedule';
import fs from 'fs';
import csv from 'csv-parser';
import FormData from 'form-data';
import logger from './logger.mjs';
import puppeteer from 'puppeteer';

// Utility to get the current file's directory
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const MAX_RETRIES = Number(process.env.BACWS_RETRY_COUNT);
const API_TIMEOUT = Number(process.env.BACWS_API_TIMEOUT) * 1000;

// Function to fetch the Cognito token for authentication
async function getCognitoToken() {
  const headers = {
    'Authorization': process.env.ABCSCUAT_BASIC_AUTH_TOKEN,
    'Content-Type': 'application/x-www-form-urlencoded'
  };
  try {
    const response = await axios.post(process.env.COGNITO_TOKEN_URL, null, { headers });
    logger.info(`CognitoTokenB2C API called. Status code: ${response.status}. Response: ${JSON.stringify(response.data)}`);
    return response.data.access_token;
  } catch (error) {
    logger.error(`Error calling CognitoTokenB2C API: ${error.message}`);
    throw error;
  }
}

// Function to fetch data from the specified API
async function fetchData(apiName, reportId) {
  const accessToken = await getCognitoToken();
  const url = `${process.env.ABCSCUAT_API_URL}/${apiName}`;
  const headers = {
    'auth-token': accessToken
  };

  try {
    const response = await axios.post(url, { report_id: reportId }, { headers });
    logger.info(`DB Query API ${apiName} called. Status Code: ${response.status}. Response: ${JSON.stringify(response.data)}`);
    return response.data;
  } catch (error) {
    logger.error(`Error calling DB Query API ${apiName}: ${error.message}`);
    if (error.response) {
      logger.error(`Error details: ${JSON.stringify(error.response.data, null, 2)}`);
    }
    throw error;
  }
}

async function saveTableAsJpeg(htmlMarkup) {
  try {
    // Launch headless browser
    const browser = await puppeteer.launch();
    const page = await browser.newPage();

    // Set content of the page
    await page.setContent(htmlMarkup);

    // Select the table
    const element = await page.$('table');

    // Take a screenshot of the table
    const jpegBuffer = await element.screenshot({ type: 'jpeg' });

    // Save the JPEG file locally
    const jpegPath = path.join(__dirname, 'table.jpg');
    fs.writeFileSync(jpegPath, jpegBuffer);

    // Close the browser
    await browser.close();

    // Return the URL of the saved image
    return jpegPath;
  } catch (error) {
    console.error(error);
  }
}

// Function to create a PDF from the data in table format
function createHtml(reportName, data) {
  const capitalizeFirstLetter = (string) => string.charAt(0).toUpperCase() + string.slice(1);

  const headers = Object.keys(data[0]);
  const capitalizedHeaders = headers.map(header => capitalizeFirstLetter(header));

  // Create the HTML structure
  let html = `
    <html>
      <head>
        <style>
          table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            font-size: 18px;
            text-align: left;
          }
          th, td {
            padding: 12px;
            border: 1px solid #ddd;
          }
          th {
            background-color: #0070d2;
            color: white;
          }
          h1 {
            color: #0070d2;
          }
        </style>
      </head>
      <body>
        <h1>${reportName}</h1>
        <table>
          <thead>
            <tr>
  `;

  // Add table headers
  capitalizedHeaders.forEach(header => {
    html += `<th>${header}</th>`;
  });

  html += `
            </tr>
          </thead>
          <tbody>
  `;

  // Add table rows
  data.forEach(row => {
    html += `<tr>`;
    headers.forEach(header => {
      html += `<td>${row[header]}</td>`;
    });
    html += `</tr>`;
  });

  html += `
          </tbody>
        </table>
      </body>
    </html>
  `;

  return html;
}


// Function to capitalize only the first letter of a string
function capitalizeFirstLetter(string) {
  return string.charAt(0).toUpperCase() + string.slice(1).toLowerCase();
}

// Function to upload file to Bag A Chat server and extract media URL
async function uploadFileToBagAChat(conversationName, filePath) {
  const headers = {
    'Authorization': process.env.BAGACHAT_API_TOKEN,
  };

  const formData = new FormData();
  formData.append('file', fs.createReadStream(filePath), { filename: path.basename(filePath) });

  try {
    const response = await axios.post('https://push.bagachat.com/api/uploadimagefortransactionalmsg.bg', formData, {
      headers: { ...formData.getHeaders(), ...headers },
      timeout: API_TIMEOUT
    });

    const mediaUrl = response.data.mediaurl;
    logger.info(`File uploaded to Bag A Chat server. Status: ${response.data.status}. Media URL: ${mediaUrl}`);

    return {
      status: response.data.status,
      mediaUrl: mediaUrl,
      mediaName: response.data.medianame,
      messageType: response.data.messagetype,
      message: response.data.message
    };
  } catch (error) {
    logger.error(`Error uploading file to Bag A Chat server: ${error.message}`);
    throw error;
  }
}

// Function to send a WhatsApp message via Bag A Chat API
async function sendWhatsAppMessage(groupId, message, pdfPath, mediaUrl, retries = 0) {
  const headers = {
    'Authorization': process.env.BAGACHAT_API_TOKEN,
    'Content-Type': 'application/json'
  };

  const requestData = {
    conversationname: groupId,
    countrycode: "+91",
    message: message,
    mediaurl: mediaUrl,
    medianame: path.basename(pdfPath)
  };

  try {
    await axios.post(process.env.WHATSAPP_URL, requestData, { headers, timeout: API_TIMEOUT });
    logger.info(`Message sent via Bag A Chat API to group ${groupId}`);
  } catch (error) {
    if (retries < MAX_RETRIES) {
      const delay = Math.pow(2, retries) * 1000; // Exponential backoff
      logger.warn(`Retrying to send message... Attempt ${retries + 1} after ${delay}ms`);
      await new Promise(resolve => setTimeout(resolve, delay));
      return sendWhatsAppMessage(groupId, message, pdfPath, mediaUrl, retries + 1);
    } else {
      logger.error(`Error sending message via Bag A Chat API to group ${groupId}: ${error.message}`);
      throw error;
    }
  }
}

// Function to process data and send a WhatsApp message
async function processData(groupId, apiName, reportName, reportId) {
  try {
    const { data, message: title } = await fetchData(apiName, reportId);

    // const { data, message:title } = {
    //   message: "Please find the Daily Txns data below ",
    //   data: [
    //     {
    //       "RPL Leads Report": "MTD-1000 LMTD 20000 %Change %20 BTD 10000"
    //     },
    //     {
    //       "RPL Leads Report": "MTD-1001 LMTD 20000 %Change %20 BTD 10000"
    //     },
    //     {
    //       "RPL Leads Report": "MTD-1002 LMTD 20000 %Change %20 BTD 10000"
    //     }
    //   ]
    // }

    // Check if the data should be sent as text or PDF
    if (data.length < 4 && Object.keys(data[0]).length < 2) {
      // Convert data to text message
      let message = data.map(row => Object.values(row).join(', ')).join('\n');
      message = `---\n ${title} \n *${reportName}* \n ${message} \n---`
      await sendWhatsAppMessage(groupId, message, '', '');
    } else {
      // Create PDF from data
      const htmlMarkup = createHtml(reportName, data);
      const dataUrl = await saveTableAsJpeg(htmlMarkup);
      console.log('dataUrl', dataUrl)
      const uploadResponse = await uploadFileToBagAChat(groupId, dataUrl);
      console.log('uploadResponse', uploadResponse)

      // Extract media URL from upload response
      const mediaUrl = uploadResponse.mediaUrl;

      await sendWhatsAppMessage(groupId, title, dataUrl, mediaUrl);
    }
  } catch (error) {
    logger.error(`Error processing data for group ${groupId}: ${error.message}`);
    throw error;
  }
}

// Function to execute the job task
async function jobTask(groupId, apiName, reportName, reportId) {
  try {
    await processData(groupId, apiName, reportName, reportId);
  } catch (error) {
    logger.error(`Error in job task for API ${apiName} and group ${groupId}: ${error.message}`);
  }
}

// Function to schedule jobs based on jobConfig
function scheduleJobs() {
  const now = new Date();

  const jobConfig = [];
  fs.createReadStream('jobConfig.csv')
    .pipe(csv())
    .on('data', (data) => jobConfig.push(data))
    .on('end', () => {
      console.log(jobConfig);
      jobConfig.forEach(job => {
        const { reportName, reportId, apiName, jobTimes, durationInMins, groupId } = job;
        const [hour, minute] = jobTimes.split(':').map(Number);
        const jobTime = new Date(now.getFullYear(), now.getMonth(), now.getDate(), hour, minute);
        const delayMinutes = (now - jobTime) / (1000 * 60);
    
        const ignoreFrequency = Number(process.env.JOB_IGNORE_FREQUENCY_MINUTES);
        const runJobPassed = Number(process.env.RUN_JOB_PASSED_MINUTES);
    
        if (durationInMins && durationInMins >= ignoreFrequency) {
          schedule.scheduleJob(`*/${durationInMins} * * * *`, () => jobTask(groupId, apiName, reportName, reportId));
        } else if (delayMinutes <= runJobPassed) {
          jobTask(groupId, apiName, reportName, reportId);
        } else {
          schedule.scheduleJob({ hour, minute }, () => jobTask(groupId, apiName, reportName, reportId));
        }
      });
    });

}

// Start job scheduling
scheduleJobs();
logger.info('Job scheduling started');
