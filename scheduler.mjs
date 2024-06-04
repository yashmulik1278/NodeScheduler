//Scheduler.js
import 'dotenv/config';
import path from 'path';
import { fileURLToPath } from 'url';
import axios from 'axios';
import schedule from 'node-schedule';
import PDFDocument from 'pdfkit';
import fs from 'fs';
import FormData from 'form-data';
import jobConfig from './jobConfig.mjs'; //jobConfig file
import logger from './logger.mjs'; //logger

const requiredEnvVars = [
  'ABCSCUAT_BASIC_AUTH_TOKEN',
  'COGNITO_TOKEN_URL',
  'ABCSCUAT_API_URL',
  'WHATSAPP_URL',
  'BAGACHAT_API_TOKEN',
  'JOB_IGNORE_FREQUENCY_MINUTES',
  'RUN_JOB_PASSED_MINUTES',
];

const missingEnvVars = requiredEnvVars.filter((envVar) => !process.env[envVar]);

if (missingEnvVars.length > 0) {
  throw new Error(`One or more required environment variables are missing: ${missingEnvVars.join(', ')}`);
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

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

async function fetchData(apiName) {
  const accessToken = await getCognitoToken();
  const url = `${process.env.ABCSCUAT_API_URL}/${apiName}`;
  console.log(url);
  const headers = {
    'Authorization': `Bearer ${accessToken}`,
  };

  try {
    const response = await axios.get(url, { headers });
    logger.info(`DB Query API ${apiName} called. Status Code: ${response.status}. Response: ${JSON.stringify(response.data)}`);
    return response.data;
  } catch (error) {
    logger.error(`Error calling DB Query API ${apiName}: ${error.message}`);
    throw error;
  }
}

async function sendWhatsappMessage(data, isPdf = false) {
  const url = `${process.env.WHATSAPP_URL}`;
  const headers = {
    'Authorization': `Bearer ${process.env.BAGACHAT_API_TOKEN}`,
    'Content-Type': 'application/json'
  };

  if (isPdf) {
    const pdfPath = path.join(__dirname, 'data.pdf');
    const doc = new PDFDocument();
    doc.pipe(fs.createWriteStream(pdfPath));
    doc.text(data);
    doc.end();

    try {
      const formData = new FormData();
      formData.append('file', fs.createReadStream(pdfPath));
      await axios.post(url, formData, { headers: { ...formData.getHeaders(), ...headers } });
      logger.info(`PDF sent via Bag A Chat API`);
    } catch (error) {
      logger.error(`Error sending PDF via Bag A Chat API: ${error.message}`);
      throw error;
    } finally {
      fs.unlinkSync(pdfPath);
    }
  } else {
    const payload = { message: data };
    try {
      await axios.post(url, payload, { headers });
      logger.info(`Message sent via Bag A Chat API`);
    } catch (error) {
      logger.error(`Error sending message via Bag A Chat API: ${error.message}`);
      throw error;
    }
  }
}

async function processData(data) {
  try {
    const rows = data.length;
    const cols = data[0] ? Object.keys(data[0]).length : 0;

    if (rows < 4 && cols < 2) {
      const textData = JSON.stringify(data, null, 2);
      await sendWhatsappMessage(textData);
    } else {
      const htmlData = `<table>${data.map(row => `<tr>${Object.values(row).map(cell => `<td>${cell}</td>`).join('')}</tr>`).join('')}</table>`;
      await sendWhatsappMessage(htmlData, true);
    }
  } catch (error) {
    logger.error(`Error processing data: ${error.message}`);
    throw error;
  }
}

async function jobTask(apiName) {
  try {
    const data = await fetchData(apiName);
    await processData(data);
  } catch (error) {
    logger.error(`Error in job task for API ${apiName}: ${error.message}`);
  }
}

function scheduleJobs() {
  const now = new Date();

  jobConfig.forEach(job => {
    const { nameOfReport, dbQueryApiName, jobTimes, durationInMins } = job;
    const [hour, minute] = jobTimes.split(':').map(Number);
    const jobTime = new Date(now.getFullYear(), now.getMonth(), now.getDate(), hour, minute);
    const delayMinutes = (now - jobTime) / (1000 * 60);

    const ignoreFrequency = Number(process.env.JOB_IGNORE_FREQUENCY_MINUTES);
    const runJobPassed = Number(process.env.RUN_JOB_PASSED_MINUTES);

    if (durationInMins && durationInMins >= ignoreFrequency) {
      schedule.scheduleJob(`*/1 * * * *`, () => jobTask(dbQueryApiName));//${durationInMins}
    } else if (delayMinutes <= runJobPassed) {
      jobTask(dbQueryApiName);
    } else {
      schedule.scheduleJob({ hour, minute }, () => jobTask(dbQueryApiName));
    }
  });
}

scheduleJobs();
logger.info('Job scheduling started');
