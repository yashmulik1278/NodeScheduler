// logger.js
import log4js from 'log4js';

log4js.configure({
  appenders: { scheduler: { type: 'file', filename: 'scheduler.log', maxLogSize: 10485760, backups: 7, compress: true } },
  categories: { default: { appenders: ['scheduler'], level: 'info' } }
});

const logger = log4js.getLogger('scheduler');

module.exports = logger;
