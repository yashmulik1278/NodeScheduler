// Scheduler.mjs
//Headers
import 'dotenv/config';
import path from 'path';
import { fileURLToPath } from 'url';
import axios from 'axios';
import schedule from 'node-schedule';
import { PDFDocument, StandardFonts, rgb } from 'pdf-lib';
import fs from 'fs';
import FormData from 'form-data';
import logger from './logger.mjs';
import jobConfig from './jobConfig.mjs';

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
async function fetchData(apiName) {
  const accessToken = await getCognitoToken();
  const url = `${process.env.ABCSCUAT_API_URL}/${apiName}`;
  const headers = {
    'auth-token': accessToken
  };

  try {
    const response = await axios.get(url, { headers });
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

// Function to create a PDF from the data in table format
async function createPdf(reportName, data) {
  const pdfDoc = await PDFDocument.create();
  let page = pdfDoc.addPage();
  const { width, height } = page.getSize();
  const fontSize = 12;

  const font = await pdfDoc.embedFont(StandardFonts.Helvetica);
  page.setFont(font);
  
  //Cell parameters
  const margin = 20;
  const tableTop = height - margin;
  const cellPadding = 5;
  const cellHeight = fontSize + cellPadding * 2;

  //Column headers
  const headers = Object.keys(data[0]);
  const capitalizedHeaders = headers.map(header => capitalizeFirstLetter(header));

  page.drawText(reportName, {
    x: margin,
    y: tableTop - fontSize - 10, // Positioning below the top margin
    size: 18,
    color: rgb(0, 0.545, 0.855),
    bold: true,
  });

  const numColumns = headers.length;
  const cellWidth = (width - margin * 2) / numColumns;

  let y = tableTop - fontSize - 40; // Start below the heading

  function splitTextIntoLines(text, maxWidth) {
    const lines = [];
    let line = '';
    const words = text.split(' ');

    for (const word of words) {
      const testLine = line + (line ? ' ' : '') + word;
      const testWidth = font.widthOfTextAtSize(testLine, fontSize);

      if (testWidth < maxWidth) {
        line = testLine;
      } else {
        lines.push(line);
        line = word;
      }
    }

    if (line) {
      lines.push(line);
    }

    return lines;
  }

  capitalizedHeaders.forEach((header, i) => {
    page.drawRectangle({
      x: margin + i * cellWidth,
      y: y - cellHeight,
      width: cellWidth,
      height: cellHeight,
      color: rgb(0, 0.545, 0.855),
    });

    page.drawText(header, {
      x: margin + i * cellWidth + cellPadding,
      y: y - cellPadding - fontSize,
      size: 14,
      color: rgb(1, 1, 1),
      bold: true
    });

    page.drawRectangle({
      x: margin + i * cellWidth,
      y: y - cellHeight,
      width: cellWidth,
      height: cellHeight,
      borderColor: rgb(0, 0, 0),
      borderWidth: 1,
    });
  });

  y -= cellHeight;

  data.forEach(row => {
    const rowHeights = headers.map(header => {
      const text = row[header].toString();
      const lines = splitTextIntoLines(text, cellWidth - 2 * cellPadding);
      return lines.length * cellHeight;
    });

    const maxRowHeight = Math.max(...rowHeights);

    if (y - maxRowHeight < margin) {
      page = pdfDoc.addPage();
      page.setFont(font);
      y = tableTop;
    }

    headers.forEach((header, i) => {
      const text = row[header].toString();
      const lines = splitTextIntoLines(text, cellWidth - 2 * cellPadding);

      lines.forEach((line, lineIndex) => {
        page.drawText(line, {
          x: margin + i * cellWidth + cellPadding,
          y: y - cellPadding - fontSize - (lineIndex * cellHeight),
          size: fontSize
        });
      });

      page.drawRectangle({
        x: margin + i * cellWidth,
        y: y - maxRowHeight,
        width: cellWidth,
        height: maxRowHeight,
        borderColor: rgb(0, 0, 0),
        borderWidth: 1,
      });
    });

    y -= maxRowHeight;
  });

  const pdfBytes = await pdfDoc.save();
  const pdfPath = path.join(__dirname, 'data.pdf');
  fs.writeFileSync(pdfPath, pdfBytes);

  return pdfPath;
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
async function processData(groupId, apiName, reportName) {
  try {
    const data = await fetchData(apiName);

    // Check if the data should be sent as text or PDF
    if (data.length < 4 && Object.keys(data[0]).length < 2) {
      // Convert data to text message
      const message = data.map(row => Object.values(row).join(', ')).join('\n');
      await sendWhatsAppMessage(groupId, message, '', '');
    } else {
      // Create PDF from data
      const pdfFilePath = await createPdf(reportName, data);
      const uploadResponse = await uploadFileToBagAChat(groupId, pdfFilePath);
      const message = `Last Call In Development:`;

      // Extract media URL from upload response
      const mediaUrl = uploadResponse.mediaUrl;

      await sendWhatsAppMessage(groupId, message, pdfFilePath, mediaUrl);
    }
  } catch (error) {
    logger.error(`Error processing data for group ${groupId}: ${error.message}`);
    throw error;
  }
}

// Function to execute the job task
async function jobTask(groupId, apiName, reportName) {
  try {
    logger.info(`Executing job for group ${groupId}, api ${apiName}, report ${reportName}`);
    await processData(groupId, apiName, reportName);
  } catch (error) {
    logger.error(`Error in job task for API ${apiName} and group ${groupId}: ${error.message}`);
  }
}

// Function to schedule jobs based on jobConfig
function scheduleJobs() {
  const now = new Date();

  jobConfig.forEach(job => {
    const { reportName, apiName, jobTimes, durationInMins, groupId } = job;
    const [hour, minute] = jobTimes.split(':').map(Number);
    const jobTime = new Date(now.getFullYear(), now.getMonth(), now.getDate(), hour, minute);
    const delayMinutes = (now - jobTime) / (1000 * 60);

    const ignoreFrequency = Number(process.env.JOB_IGNORE_FREQUENCY_MINUTES);
    const runJobPassed = Number(process.env.RUN_JOB_PASSED_MINUTES);

    if (durationInMins && durationInMins >= ignoreFrequency) {
      schedule.scheduleJob(`*/${durationInMins} * * * *`, () => jobTask(groupId, apiName, reportName));
    } else if (delayMinutes <= runJobPassed) {
      jobTask(groupId, apiName, reportName);
    } else {
      schedule.scheduleJob({ hour, minute }, () => jobTask(groupId, apiName, reportName));
    }
  });
}

// Start job scheduling
scheduleJobs();
logger.info('Job scheduling started');