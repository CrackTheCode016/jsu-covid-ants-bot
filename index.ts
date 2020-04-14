import { CovidReportingBot } from './src/bot'
import * as env from 'dotenv'

env.config()

console.log(process.env.KEY)
const bot = new CovidReportingBot(
    'http://198.199.80.167:3000',
    process.env.KEY!)

bot.start()
