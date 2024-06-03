import path from 'path';
import axios from 'axios';
import schedule from 'node-schedule';
import PDFDocument from 'pdfkit';
import config from './config'; //config file
import jobConfig from './jobConfig'; //jobConfig file
import logger from './logger'; //logger

// Define the application configuration
const appConfig = {
  API_TOKEN: config.API_TOKEN,
  WHATSAPP_URL: config.WHATSAPP_URL,
  WHATSAPP_PHONE_API: config.WHATSAPP_PHONE_API,
  WHATSAPP_GROUP_API: config.WHATSAPP_GROUP_API,
  WHATSAPP_IMAGE_API: config.WHATSAPP_IMAGE_API,
  DB_QUERY_API_ENDPOINT: config.DB_QUERY_API_ENDPOINT,
  JOB_IGNORE_FREQUENCY_MINUTES: config.JOB_IGNORE_FREQUENCY_MINUTES,
  RUN_JOB_PASSED_MINUTES: config.RUN_JOB_PASSED_MINUTES,
  BACWS_API_TIMEOUT: config.BACWS_API_TIMEOUT,
  BACWS_RETRY_COUNT: config.BACWS_RETRY_COUNT,
  COGNITO_TOKEN_URL: config.COGNITO_TOKEN_URL,
  BAGCHAT_API_TOKEN: config.BAGCHAT_API_TOKEN
};

async function getCognitoToken(){
  const headers = {
    'Authorization': appConfig.BASIC_AUTH_TOKEN,
    'Content-Type': 'application/x-www-form-urlencoded'
  };

  try{
    const response = await axios.post(appConfig.COGNITO_TOKEN_URL, null, {headers});
    logger.info(`CognitoTokenB2C API called. Status code: ${response.status}. Response: ${JSON.stringify(response.data)}`);
    return response.data.access_token;
  }catch(error){
    logger.error(`Error calling CognitoTokenB2C API: ${error.message}`);
    throw error;
  }
}

async function fetchData(apiName) {
  const accessToken = await getCognitoToken();
  const url = `${appConfig.DB_QUERY_API_ENDPOINT}${apiName}`;
  const headers = {
    'Authorization': `Bearer ${accessToken}`,
  };

  try {
    const response = await axios.get(url, {headers});
    logger.info(`DB Query API ${apiName} called. Status Code: ${response.status}. Response: ${JSON.stringify(response.data)}`);
    return response.data;
  } catch (error) {
    logger.error(`Error calling DB Query API ${apiName}: ${error.message}`);
    throw error;
  }
}

async function sendWhatsappMessage(data, isPdf = false) {
  const url = `${appConfig.WHATSAPP_URL}${appConfig.WHATSAPP_PHONE_API}`;
  const headers = {
    'Authorization': `Bearer ${appConfig.BAGCHAT_API_TOKEN}`,
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
  const rows = data.length;
  const cols = data[0] ? Object.keys(data[0]).length : 0;

  if (rows < 4 && cols < 2) {
    const textData = JSON.stringify(data, null, 2);
    await sendWhatsappMessage(textData);
  } else {
    const htmlData = `<table>${data.map(row => `<tr>${Object.values(row).map(cell => `<td>${cell}</td>`).join('')}</tr>`).join('')}</table>`;
    await sendWhatsappMessage(htmlData, true);
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

    if (durationInMins && durationInMins >= appConfig.JOB_IGNORE_FREQUENCY_MINUTES) {
      schedule.scheduleJob(`*/${durationInMins} * * * *`, () => jobTask(dbQueryApiName));
    } else if (delayMinutes <= appConfig.RUN_JOB_PASSED_MINUTES) {
      jobTask(dbQueryApiName);
    } else {
      schedule.scheduleJob({ hour, minute }, () => jobTask(dbQueryApiName));
    }
  });
}

scheduleJobs();
logger.info('Job scheduling started');
