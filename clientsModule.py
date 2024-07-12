import datetime
import imbox
import sqlite3  as sql
import pandas   as pd
import datetime as dt

dbName = 'agv.db'

class client:
    '''
    This class defines an active client of AGV for the energy management service
    '''
    def __init__(self, name, address, CEP, cnpj, email, phone, legalPerson, paymentMethod = None) -> None:
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

    def totalConsumption(self, month, consumptionType, year = None) -> int:
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

        typeDict = {
            'consumption':'consumo',
            'peak-consumption':'consumo',
            'off-peakpconsumption':'consumo',
            'demand':'demanda',
            'peak-demand':'demanda',
            'off-peak-demand':'demanda'
        }

        hourDict = {
            'consumption':'Nao se aplica',
            'peak-consumption':'Ponta',
            'off-peakpconsumption':'Fora ponta',
            'demand':'Nao se aplica',
            'peak-demand':'Ponta',
            'off-peak-demand':'Fora ponta'
        }

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
    

    def totalCosts(self, month) -> float:
        '''
            This method calculates the costs that would be if the UCs linked to the client had no savings in a reference month
            :param month: A reference month

            :return: float
        '''
        pass

    def totalSavings(self, month):
        '''
            This method calculates the savings of all the UCs linked to the client in a reference month
        '''        
        pass

    def linkUC(self, ucNumber):
        pass

    def unlinkUC(self, ucNumber):
        pass

    def createSavingsReport(self, month):
        '''
            This method creates a saving report for all the UCs linked to the client in a reference month
        '''
        pass

    def sendSavingsReport(self,archive):
        '''
            This method sends an archive which must be a saving report created previously
        '''
        pass

class uc:
    def __init__(self, utility, number, client, address, CEP, subgroup, modality, peakDemand = None, offPeakDemand = None, demand = None) -> None:
        self.utility        = utility
        self.number         = number
        self.client         = client
        self.address        = address
        self.CEP            = CEP
        self.subgroup       = subgroup
        self.modality       = modality
        self.peakDemand     = peakDemand
        self.offPeakDemand  = offPeakDemand
        self.Demand         = demand
    
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
        pass
    
    def readUC(self) -> int:
        pass

    def updateUC(self) -> int:
        pass

    def deleteUC(self) -> int:
        pass

    def createValue(self, month, valueType) -> int:
        '''
        This method will create a register of consumption or demand for this UC for a specific month
        valueType can be:
            1. consumption
            2. peak-consumption
            3. off-peak-consumption
            4. demand
            5. peak-demand
            6. off-peak-demand
        '''
        pass

    def readValue(self, month, valueType) -> int:
        '''
        This method will read a register of consumption or demand for this UC for a specific month
        valueType can be:
            1. consumption
            2. peak-consumption
            3. off-peak-consumption
            4. demand
            5. peak-demand
            6. off-peak-demand
        '''
        pass

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
    
    def totalCosts(self, month) -> float:
        '''
        This method calculates the total cost for a reference month in this UC
        '''
        pass

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
    