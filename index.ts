import { CovidReportingBot } from './src/bot'
import * as env from 'dotenv'

env.config()

console.log(process.env.KEY)
const bot = new CovidReportingBot(
    'http://178.128.184.107:3000',
    process.env.KEY!)

bot.start()
