import requests
import pandas as pd
from time import sleep


def searchCities(url, reqParams):
    response = requests.request("GET", url, params=reqParams)
    cities = response.json()
    return cities


def convertTempFtoCelsius(temp):
    result = (temp - 32) / 1.8
    return result


def calculateRainProbability(day, night):
    result = (day + night) / 2
    return result


def calculateMaxSpeedWind(item):
    windSpeedDay = item["Day"]["Wind"]["Speed"]["Value"]
    windSpeedNight = item["Night"]["Wind"]["Speed"]["Value"]
    windSpeedFormat = item["Day"]["Wind"]["Speed"]["Unit"]

    if (windSpeedDay > windSpeedNight):
        result = str(windSpeedDay) + " " + windSpeedFormat
        return result

    result = str(windSpeedNight) + " " + windSpeedFormat
    return result


def buildDfCities(response):
    citiesNames = []
    for item in response:
        citiesNames.append([item['nome']])
    dfCities = pd.DataFrame(data=citiesNames, columns=['Names of cities'])
    return dfCities


def builderRowsToDF1(arrayForecast, id, cityName, region, country):
    dataArrays = []
    countDays = 0
    countDaysWillRain = 0
    for item in arrayForecast:
        date = item["Date"]
        countDays += 1
        willRain = "Não"
        latitude = ""
        longitude = ""
        tempMaxF = int(item["Temperature"]["Maximum"]["Value"])
        tempMinF = int(item["Temperature"]["Minimum"]["Value"])
        tempMaxC = convertTempFtoCelsius(tempMaxF)
        tempMinC = convertTempFtoCelsius(tempMinF)
        tempAverage = (tempMaxC + tempMinC) / 2
        tempMaxC = '{:.1f}'.format(tempMaxC) + " °C"
        tempMinC = '{:.1f}'.format(tempMinC) + " °C"
        tempAverage = '{:.1f}'.format(tempAverage) + " °C"

        rainProbabilityDay = item["Day"]["RainProbability"]
        rainProbabilityNight = item["Night"]["RainProbability"]
        rainProbability = calculateRainProbability(
            rainProbabilityDay, rainProbabilityNight)

        if(rainProbability > 0 and item["Day"]["HasPrecipitation"] == "true" and item["Night"]["HasPrecipitation"] == "true"):
            precipitationTypeDay = item["Day"]["PrecipitationType"]
            precipitationTypeNigth = item["Night"]["PrecipitationType"]
            if (precipitationTypeDay == "Rain" or precipitationTypeNigth == "Rain"):
                willRain = "Sim"
        elif(rainProbability > 30):
            willRain = "Sim"

        tempConditionDay = item["Day"]["LongPhrase"]
        tempConditionNigth = item["Night"]["LongPhrase"]
        sunRise = item["Sun"]["Rise"][11:16]
        maxWindSpeed = calculateMaxSpeedWind(item)
        if (willRain == "Sim"):
            countDaysWillRain += 1
        dataArrays.append([id, cityName, date[0:10], region, country, latitude,
                           longitude, tempMaxC, tempMinC, tempAverage, willRain, rainProbability,
                           tempConditionDay, tempConditionNigth, sunRise, "", maxWindSpeed])
    return dataArrays, countDays, countDaysWillRain


def builderRowToDf2(cityName, qtdDiasVaiChover, totalDiasMapeados):
    qtdDiasNaoVaiChover = totalDiasMapeados - qtdDiasVaiChover
    rowDf2 = [cityName, qtdDiasVaiChover,
              qtdDiasNaoVaiChover, totalDiasMapeados]
    return rowDf2


def buildersDfs(response):
    apiKey = "eszsYTnjGTpcyOjnnAPlGtWCCftvXDyT"
    columnsDF1 = ['CodigoDaCidade', 'Cidade', 'Data', 'Região', 'País', 'Latitude', 'Longitude', 'TemperaturaMáxima', 'TemperaturaMínima',
                  'TemperaturaMédia', 'VaiChover', 'ChanceDeChuva', 'CondicaoDoTempoDia', 'CondicaoDoTempoNoite', 'NascerDoSol', 'PorDoSol', 'VelocidadeMaximaDoVento']
    columnsDF2 = ["Cidade", "QtdDiasVaiChover",
                  "QtdDiasNaoVaiChover", "TotalDiasMapeados"]
    df1 = pd.DataFrame(columns=columnsDF1)
    df2 = pd.DataFrame(columns=columnsDF2)

    count = 0
    for city in response:
        count = count + 1
        cityId = city['id']
        cityName = city['nome']
        cityRegion = city['microrregiao']['mesorregiao']['UF']['regiao']['nome']
        country = "Brazil"
        link = f"http://dataservice.accuweather.com/forecasts/v1/daily/5day/" + \
            cityId+"?apikey="+apiKey+"&details=true&language=pt-br"
        try:
            arrayForecast = requests.get(link).json()
            rowsCities, countDays, countDaysWillRain = builderRowsToDF1(arrayForecast["DailyForecasts"],
                                                                        cityId, cityName, cityRegion, country)
            for rowDf1 in rowsCities:
                df1.loc[len(df1)] = rowDf1
            rowDf2 = builderRowToDf2(cityName, countDaysWillRain, countDays)
            df2.loc[len(df2)] = rowDf2
            print("Getting the data of city #" + str(count))
            sleep(3)
        except:
            print("Error: "+arrayForecast["Message"])
            if(arrayForecast["Message"] == "The allowed number of requests has been exceeded."):
                print("Please create another apiKey.")
            break    
    df1.to_csv("n2Table1.csv", index="false")
    df2.to_csv("n2Table2.csv", index="false")
    return


url = 'https://servicodados.ibge.gov.br/api/v1/localidades/mesorregioes/3513/municipios'
citiesResponse = searchCities(url, {})
dfCity = buildDfCities(citiesResponse)
dfCity.to_csv("Cities.csv", index="false")
buildersDfs(citiesResponse)
