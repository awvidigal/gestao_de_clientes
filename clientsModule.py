import imbox
import sqlite3  as sql
import pandas   as pd
import datetime as dt
from datetime import datetime

dbName = 'agv.db'
typeDict = {
            'consumption':'consumo',
            'peak-consumption':'consumo',
            'off-peak-consumption':'consumo',
            'demand':'demanda',
            'peak-demand':'demanda',
            'off-peak-demand':'demanda'
        }

hourDict = {
            'consumption':'Nao se aplica',
            'peak-consumption':'Ponta',
            'off-peak-consumption':'Fora ponta',
            'demand':'Nao se aplica',
            'peak-demand':'Ponta',
            'off-peak-demand':'Fora ponta'
        }

monthDict = {
            'jan':1,
            'fev':2,
            'mar':3,
            'abr':4,
            'mai':5,
            'jun':6,
            'jul':7,
            'ago':8,
            'set':9,
            'out':10,
            'nov':11,
            'dez':12 
        }

valueTypesList = [
    'consumption',
    'peak-consumption',
    'off-peak-consumption',
    'demand',
    'peak-demand',
    'off-peak-demand'
]

bGroupList = [
    'B1',
    'B2',
    'B3',
    'B4'
]

aGroupList = [
    'A1',
    'A2',
    'A3',
    'A3a',
    'A4',
    'A5'
]

modalityList = [
    'Azul',
    'Branca',
    'Convencional',
    'Convencional pré-pagamento',
    'Distribuição',
    'Geração',
    'Verde'
]


class client:
    '''
    This class defines an active client of AGV for the energy management service
    '''
    def __init__(self, name: str, address: str, CEP: str, cnpj: str, email: str, phone: str, legalPerson: str, paymentMethod = None) -> None:
        '''
        Constructor of the class client

        :param name: The name that identifies the client. Preferably the name in the energy bill
        :param address: The address of the client. Not necessarily the same address of any UC linked to him. Remember to fill the complete address, except the CEP, like in google maps
        :param CEP: The CEP of the address. This param is a string
        :param cnpj: The document of the client. CNPJ if he's a company, or a CPF if he's a person
        :param email: The recipient email address where all the comunication will be sent 
        :param phone: The pphone number of the client. Preferably a whatsapp number
        :param legalPerson: The person type. If is a company or a person. It accepts the following values:
            - 'person'
            - 'company'

        :return: None
        '''
        self.name           = name
        self.address        = address
        self.CEP            = CEP
        self.cnpj           = cnpj
        self.email          = email
        self.phone          = phone
        self.legalPerson    = legalPerson
        self.paymentMethod  = paymentMethod
        self.ucList         = []
        
    def createClient(self) -> None:
        '''
        This method will include the client in the clients database
        '''

        verification = self.readClient()
        date = dt.datetime.now()
        if not verification:
            conn = sql.connect(dbName)
            cursor = conn.cursor()

            cursor.execute(
                '''
                INSERT INTO clientes (nome, endereco, cep, cnpj, email, telefone, responsavel_legal, forma_de_pagamento, created_at)
                VALUES (?,?,?,?,?,?,?,?)
                ''', (self.name, self.address, self.CEP, self.cnpj, self.email, self.phone, self.legalPerson, self.paymentMethod, date)
            )
            conn.commit()
            conn.close()

    def readClient(self):
        '''
        This method will read the client's data in the client's database
        '''
        conn = sql.connect(dbName)
        cursor = conn.cursor()

        results = cursor.execute(
            '''
            SELECT * 
            FROM clientes
            WHERE cnpj = ?
            ''', (self.cnpj,)
        ).fetchone()
        conn.close()

        return results

    def updateClient(self) -> None:
        pass

    def deleteClient(self) -> None:
        pass

    def totalConsumption(self, month: str, consumptionType: str, year = None) -> int:
        '''
            This method calculates the consumption of all the UCs linked to this client in a reference month 

            :param  month: A reference month for the consumption
            :param consumptionType: What type of value should be readfor the referenced month. It accepts the following values:
                - 'consumption'
                - 'peak-consumption'
                - 'off-peak-consumption'
                - 'demand'
                - 'peak-demand'
                - 'off-peak-demand'
            :param year: A reference year. If None, then the method will consider the current year
            :return: int
        '''
        total = None

        if not year:
            year = dt.datetime.now().year
        
        conn = sql.connect(dbName)
        cursor = conn.cursor()

        clientID = cursor.execute(
            '''
            SELECT id
            FROM clientes
            WHERE cnpj = ?
            ''', (self.cnpj,)
        ).fetchone()

        elecTariff = cursor.execute(
            '''
            SELECT id
            FROM posto
            WHERE descricao = ?
            ''', (hourDict[consumptionType], )
        ).fetchone()

        if typeDict[consumptionType] == 'consumo':
            value = cursor.execute(
                '''
                SELECT valor
                FROM consumo
                WHERE mes = ?, ano = ?, posto_id = ?, cliente_id = ?
                ''', (month, year, elecTariff, clientID)
            ).fetchall()

        else:
            value = cursor.execute(
                '''
                SELECT valor
                FROM demanda
                WHERE mes = ?, ano = ?, posto_id = ?, cliente_id = ?
                ''', (month, year, elecTariff, clientID)
            ).fetchall()
            
        conn.close()

        for unit in value:
            total += value[unit]

        return total
    

    def totalCosts(self, month: str) -> float:
        '''
            This method calculates the costs that would be if the UCs linked to the client had no savings in a reference month
            :param month: A reference month

            :return: float
        '''
        pass

    def totalSavings(self, month: str):
        '''
            This method calculates the savings of all the UCs linked to the client in a reference month
        '''        
        pass

    def linkUC(self, ucNumber: str):
        pass

    def unlinkUC(self, ucNumber: str):
        pass

    def createSavingsReport(self, month: str):
        '''
            This method creates a saving report for all the UCs linked to the client in a reference month
        '''
        pass

    def sendSavingsReport(self,archive: str):
        '''
            This method sends an archive which must be a saving report created previously
        '''
        pass

class uc:
    def __init__(self, utility, number, client, address, CEP, subgroup, modality, clientClass, peakDemand = None, offPeakDemand = None, demand = None) -> None:
        self.utility        = utility
        self.number         = number
        self.client         = client
        self.address        = address
        self.CEP            = CEP
        self.subgroup       = subgroup
        self.modality       = modality
        self.peakDemand     = peakDemand
        self.offPeakDemand  = offPeakDemand
        self.demand         = demand
        self.clientClass    = clientClass
    
    '''
 ## ##   ### ##   ##  ###  ### ##             ## ##   #### ##    ##     ### ##   #### ##  
##   ##   ##  ##  ##   ##   ##  ##           ##   ##  # ## ##     ##     ##  ##  # ## ##  
##        ##  ##  ##   ##   ##  ##           ####       ##      ## ##    ##  ##    ##     
##        ## ##   ##   ##   ##  ##            #####     ##      ##  ##   ## ##     ##     
##        ## ##   ##   ##   ##  ##               ###    ##      ## ###   ## ##     ##     
##   ##   ##  ##  ##   ##   ##  ##           ##   ##    ##      ##  ##   ##  ##    ##     
 ## ##   #### ##   ## ##   ### ##             ## ##    ####    ###  ##  #### ##   ####    
    '''      

    def createUC(self) -> int:
        '''
        This method create an UC in the database. It returns a RunTimeError if you try to create an existent UC, and returns 0 if everything runs ok
        '''
        ucValidation = self.readUC()

        if not ucValidation:
            date = dt.datetime.now()
            
            conn = sql.connect(dbName)
            cursor = conn.cursor()

            clientID = cursor.execute(
                '''
                SELECT id
                FROM clientes
                WHERE nome = ?
                ''', (self.client,)
            ).fetchone()
            
            cursor.execute(
                '''
                INSERT INTO ucs (numero, concessionaria, client_id, endereco, cep, subgrupo, modalidade, classe, demanda, demanda_ponta, demanda_fora_ponta, created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ''', (self.number, self.utility, clientID, self.address, self.CEP, self.subgroup, self.modality, self.clientClass, self.demand, self.peakDemand, self.offPeakDemand, date)
            )
            conn.commit()
            conn.close()

        else:
            raise RuntimeError('Essa UC já está cadastrada')

        return 0
    
    def readUC(self):
        conn = sql.connect(dbName)
        cursor = conn.cursor()

        ucRegister = cursor.execute(
            '''
            SELECT *
            FROM ucs
            WHERE numero = ?
            ''', (self.number,)
        ).fetchone()

        conn.close()

        return ucRegister

    def updateUC(self) -> int:
        pass

    def deleteUC(self) -> int:
        pass

    def createValue(self, month, valueType, value, year = None) -> int:
        '''
        This method will create a register of consumption or demand for this UC for a specific month
        :param month: A reference month
        :param valueType: What type of value will be registered. It accepts the following values
            1. consumption
            2. peak-consumption
            3. off-peak-consumption
            4. demand
            5. peak-demand
            6. off-peak-demand
        :param value: The value that'll be registered
        :param year: A reference year. If not filled, it'll be the current year
        '''
        
        if valueType in valueTypesList:
            existsValue = self.readValue(month, valueType, year)
            date = dt.datetime.now()

            if not year:
                year = date.year

            if not existsValue:
                ucReg = self.readUC()
                ucID = ucReg[0]

                conn = sql.connect(dbName)
                cursor = conn.cursor()

                postoID = cursor.execute(
                    '''
                    SELECT id
                    FROM posto_id
                    WHERE descricao = ?
                    ''', (hourDict[valueType], )
                )

                if typeDict[valueType] == 'demanda':
                    cursor.execute(
                        '''
                        INSERT INTO demandas (uc_id, posto_id, mes, ano, valor, created_at)
                        VALUES (?,?,?,?,?,?)
                        ''', (ucID, postoID, month, year, value, date)
                    )

                else:
                    cursor.execute(
                        '''
                        INSERT INTO consumos (uc_id, posto_id, mes, ano, valor, created_at)
                        VALUES (?,?,?,?,?,?)
                        ''', (ucID, postoID, month, year, value, date)
                    )

                conn.commit()
                conn.close()

            else:
                raise RuntimeError('This value is already registered. Please update it or leave it')
        
        return 0


    def readValue(self, month: str, valueType: str, year: int = None) -> int:
        '''
        This method will read a register of consumption or demand for this UC for a specific month

        Parameters
        ----------
        month : A reference month
            The pattern is the first three letters of the month, in portuguese, with small letters
        
        year : A reference year, like an integer.
            If the value is not filled, the function will consider the current year

        valueType : What type of value will be registered. 
            It accepts the following values: 'consumption', 'peak-consumption', 'off-peak-consumption', 'demand', 'peak-demand', 'off-peak-demand'
        '''

        if valueType in valueTypesList:
            conn = sql.connect(dbName)
            cursor = conn.cursor()

            date = dt.datetime.now()

            if not year:
                year = date.year

            if typeDict[valueType] == 'consumo':
                value = cursor.execute(
                    '''
                    SELECT *
                    FROM consumos
                    WHERE mes=?, ano=?
                    ''', (month, year,)
                ).fetchone()
            else:
                value = cursor.execute(
                    '''
                    SELECT *
                    FROM demandas
                    WHERE mes=?, ano=? 
                    ''', (month, year,)
                ).fetchone()

            return value
        
        else:
            raise TypeError('valueType param not recognized') 
        
        

    def updateValue(self, month, valueType) -> int:
        '''
        This method will update a register of consumption or demand for this UC for a specific month
        valueType can be:
            1. consumption
            2. peak-consumption
            3. off-peak-consumption
            4. demand
            5. peak-demand
            6. off-peak-demand
        '''
        pass
    
    def deleteValue(self, month, valueType) -> int:
        '''
        This method will delete a register of consumption or demand for this UC for a specific month
        valueType can be:
            1. consumption
            2. peak-consumption
            3. off-peak-consumption
            4. demand
            5. peak-demand
            6. off-peak-demand
        '''
        pass

    '''
 ## ##   ### ##   ##  ###  ### ##            ### ###    ####   ###  ##    ####    ## ##   ###  ##  
##   ##   ##  ##  ##   ##   ##  ##            ##  ##     ##      ## ##     ##    ##   ##   ##  ##  
##        ##  ##  ##   ##   ##  ##            ##         ##     # ## #     ##    ####      ##  ##  
##        ## ##   ##   ##   ##  ##            ## ##      ##     ## ##      ##     #####    ## ###  
##        ## ##   ##   ##   ##  ##            ##         ##     ##  ##     ##        ###   ##  ##  
##   ##   ##  ##  ##   ##   ##  ##            ##         ##     ##  ##     ##    ##   ##   ##  ##  
 ## ##   #### ##   ## ##   ### ##            ####       ####   ###  ##    ####    ## ##   ###  ##    
    '''
    
    def monthlyCosts(self, month: str, year: int = None) -> float:
        '''
        This method calculates the total cost for a reference month in this UC, disregarding taxes like ICMS, PIS/COFINS

        Parameters
        ----------

        month : str
            A reference month. The pattern is the first three letters of the month, in portuguese, with small letters
        
        year : int
            A reference year, like an integer. If the value is not filled, the function will consider the current year

        return : float
        '''
        # verificar se as tarifas para o período existem no db
        
        peakDemand          = None
        offPeakDemand       = None
        demand              = None
        peakConsumption     = None
        offPeakConsumption  = None
        consumption         = None
        monthlyCosts        = None

        tusd    = None
        te      = None
        
        minDate = datetime(year, monthDict[month], 1)
        if month == 'fev':
            maxDate = datetime(year, monthDict[month], 28)

        else:
            maxDate = datetime(year, monthDict[month], 30)

        conn = sql.connect(dbName)
        cursor = conn.cursor()

        # Baixa as tarifas filtradas por concessionaria, subgrupo, modalidade e classe
        tariffs = cursor.execute(
            '''
            SELECT * 
            FROM tarifas
            WHERE concessionaria=?, modalidade=?, subgrupo=?, classe=?
            ''', (self.utility, self.modality, self.subgroup, self.clientClass)
        ).fetchall()

        conn.close()

        # Transforma as colunas de data em formato datetime
        for item in tariffs:
            tariffs[1] = dt.datetime.strptime(tariffs[1])
            tariffs[2] = dt.datetime.strptime(tariffs[2])

        # Transforma a a lista de tarifas em um dataframe e filtra pela data da REH
        tariffs = pd.DataFrame(data=tariffs)
        tariffs.query(f'inicio_vigencia <= {minDate} and fim_vigencia >= {maxDate}', inplace=True)
        tariffs.reset_index(drop=True, inplace=True)

        if not tariffs:
            raise RuntimeError('Please, update the prices table for this utility')
            return 0
        
        else:
            demandsPrice        = tariffs.query("unidade == 'R$/kW'")
            consumptionsPrice   = tariffs.query("unidade == 'R$/MWh'")
            
            demandPrice         = demandsPrice.query("posto == 'Nao se aplica'")
            demandPrice         = demandPrice.loc['0', 'tusd']
            peakDemandPrice     = demandsPrice.query("posto == 'Ponta'")
            peakDemandPrice     = peakDemandPrice.loc['0', 'tusd']
            offPeakDemandPrice  = demandsPrice.query("posto == Fora ponta")
            offPeakDemandPrice  = offPeakDemandPrice.loc['0', 'tusd']

            consumptionPrice        = consumptionsPrice.query("posto == 'Nao se aplica'")
            consumptionPrice        = consumptionPrice.loc['0', 'te'] + consumptionPrice.loc['0', 'tusd']
            peakConsumptionPrice    = consumptionsPrice.query("posto == 'Ponta'")
            peakConsumptionPrice    = peakConsumptionPrice.loc['0', 'te'] + peakConsumptionPrice.loc['0', 'tusd']
            offPeakConsumptionPrice = consumptionsPrice.query("posto == 'Fora ponta'")
            offPeakConsumptionPrice = offPeakConsumptionPrice.loc['0', 'te'] + offPeakConsumptionPrice.loc['0', 'tusd']
            
            if self.subgroup in bGroupList:
                tusd    = tariffs.loc['0', 'tusd']
                te      = tariffs.loc['0', 'te']

                consumption = self.readValue(month, 'consumption', year)

                monthlyCosts = consumption * (tusd + te) / 1000

            elif self.subgroup in aGroupList:
                peakConsumption     = self.readValue(month, 'peak-consumption', year)
                offPeakConsumption  = self.readValue(month, 'off-peak-consumption', year)
                
                peakDemand      = self.readValue(month, 'peak-demand', year)
                offPeakDemand   = self.readValue(month, 'off-peak-demand', year)
                demand          = self.readValue(month, 'demand', year)

                monthlyCosts = (
                    peakConsumption * (peakConsumptionPrice / 1000) + 
                    offPeakConsumption * (offPeakConsumptionPrice / 1000) +
                    demand * demandPrice +
                    peakDemand * peakDemandPrice +
                    offPeakDemand * offPeakDemandPrice
                )

            else:
                raise RuntimeError('Subgroup not finded')
                return 0
            
            return monthlyCosts


    def totalSavings(self, month) -> float:
        '''
        This method calculates the total savings for a reference month in this UC
        '''
        pass

    def createReport(self, period) -> str:
        '''
            This method creates a saving report for the UC in a reference period
        '''
        pass

    def sendReport(self, archive) -> int:
        '''
            This method sends an archive which must be a saving report
        '''
        pass

    def linkClient(self, document) -> int:
        '''
            This method links the UC to a pre registered client.
            The client will be referenced by its document
        '''
        pass
    def unlinkClient(self) -> int:
        '''
            This method unlinks the UC from a pre linked client. 
        '''
        pass
    