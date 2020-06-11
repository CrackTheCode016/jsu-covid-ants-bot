import { ICOVID19GeneralReport } from './model/ICOVID19'
import csvParser, * as parser from 'csv-parser'
import { Readable } from 'stream'
import * as cron from 'cron'
import * as fetch from 'node-fetch'
import { DataReport, ReportDataPoint, ArchiveHttp, ReportHttp } from 'ants-protocol-sdk'
import { TransactionAnnounceResponse, Account, NetworkType, RepositoryFactoryHttp } from 'symbol-sdk'

export class CovidReportingBot {

    private jhuBaseUrl = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'
    private reportHttp: ReportHttp
    private account: Account
    private formatMonth = (date: Date) => {
        const month = (date.getMonth() + 1).toString()
        console.log(month.charAt(0))
        return month.charAt(0) == '1' ? month.toString() : `0${month.toString()}`
    }
    private formatDay = (date: Date) => {
        const day = (date.getDate()).toString()
        console.log(day)
        return parseInt(day) > 10 ? day.toString() : `0${day.toString()}`
    }

    constructor(
        nodeUrl: string,
        apiKey: string
    ) {
        const repositoryFactor = new RepositoryFactoryHttp(nodeUrl)
        this.reportHttp = new ReportHttp(repositoryFactor)
        this.account = Account.createFromPrivateKey(apiKey, NetworkType.TEST_NET)
    }

    public start() {
        const night = '59 23 * * *'
        const minute = '* * * * *'
        cron.job({
            cronTime: night, onTick: () => {
                const date = new Date()
                const calculateDateFormatted: string = `${this.formatMonth(date)}-${this.formatDay(date)}-${date.getFullYear()}.csv`
                console.log(calculateDateFormatted)
                const url = this.jhuBaseUrl + calculateDateFormatted
                this.fetchData(url)
                    .then((reports) => {
                        return this.sendReportToNode(reports)
                    }).then((response) => console.log(response))
            }
        }).start()
    }

    private sendReportToNode(jhuResponse: ICOVID19GeneralReport[]) {
        let totalGlobalInfected: number = 0
        let totalGlobalDeathCount: number = 0
        let totalGlobalRecovered: number = 0
        let totalGlobalActive: number = 0
        jhuResponse.forEach((report) => {
            totalGlobalDeathCount += parseInt(report.deaths.toString())
            totalGlobalInfected += parseInt(report.confirmed.toString())
            totalGlobalRecovered += parseInt(report.recovered.toString())
        })
        totalGlobalActive = totalGlobalInfected - totalGlobalRecovered
        const perCountryReports = ReportDataPoint.fromInterfaceToDataPoint('covidCountryReport', jhuResponse, true)
        const totalGlobalInfectedPoint = new ReportDataPoint('totalGlobalInfected', totalGlobalInfected)
        const totalGlobalDeathCountPoint = new ReportDataPoint('totalGlobalDeathCount', totalGlobalDeathCount)
        const totalGlobalRecoveredPoint = new ReportDataPoint('totalGlobalRecovered', totalGlobalRecovered)
        const totalGlobalActivePoint = new ReportDataPoint('totalGlobalActive', totalGlobalActive)
        const finalPoints: ReportDataPoint[] = [totalGlobalInfectedPoint, totalGlobalDeathCountPoint, totalGlobalRecoveredPoint, totalGlobalActivePoint, perCountryReports]
        const dataReport = new DataReport(
            'covid',
            'https://github.com/CSSEGISandData/COVID-19',
            finalPoints,
            this.account.address)
        return this.reportHttp
            .announceReportToArchive(
                this.account,
                dataReport,
                'covid',
                'covidtrackertest').toPromise()
    }

    private fetchData(url: string): Promise<ICOVID19GeneralReport[]> {
        return fetch.default(url)
            .then((response: any) => {
                if (!response.ok) {
                    console.log(response.statusText)
                }
                return response.text()
            }).then((responseText: string) => {
                return this.parseDataFromCsv(responseText)
            })
    }

    private parseDataFromCsv(text: string): Promise<ICOVID19GeneralReport[]> {
        let result: ICOVID19GeneralReport[] = []
        return new Promise<ICOVID19GeneralReport[]>((resolve, reject) => {
            Readable.from(text)
                .pipe(csvParser())
                .on('data', (data) => {
                    const obj: ICOVID19GeneralReport = {
                        province_state: `${data["Admin2"]} ${data.Province_State}`,
                        last_update: data.Last_Update,
                        country_region: data.Country_Region,
                        combined: data.Combined_Key,
                        active: parseInt(data.Active),
                        confirmed: parseInt(data.Confirmed),
                        deaths: parseInt(data.Deaths),
                        recovered: parseInt(data.Recovered),
                        latitude: data.Lat,
                        longitude: data["Long_"]
                    }
                    result.push(obj)
                }).on('end', () => {
                    console.log(result)
                    resolve(result)
                })
        })

    }
}