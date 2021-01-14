from flask import Flask, render_template, jsonify, request, abort
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import os, json
import logging
import configparser
import traceback
from sqlalchemy.exc import OperationalError
import sys

logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d \
:: %(message)s', level = logging.INFO)

config = configparser.ConfigParser()
config.read('database.cfg')
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://{}:{}@localhost:{}/{}'.format(*config['DATABASE'].values())
# Suppress deprecation warning
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)

class Customer(db.Model):
    __tablename__ = 'customer'

    cust_id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False)
    email = db.Column(db.String(50), nullable=False)
    phone = db.Column(db.String(50), nullable=False)
    bankAccount = db.relationship(
        "BankAccount", backref=db.backref("customer", lazy="joined"), lazy="select"
    )

class BankAccount(db.Model):

    __tablename__ = "bankaccount"

    bank_acc_no = db.Column(db.Integer, primary_key=True)
    cust_id = db.Column(db.Integer,db.ForeignKey("customer.cust_id"))
    actnType= db.Column(db.String(50),nullable='False') 
    money= db.Column(db.Float,nullable='False') 

    def deposit(self,cust_id,amount):
        """      
        The function to deposit a amount to a Bank Account.
       
        Parameters:
               amount (Integer): The amount to be deposited
       
        Returns:
               Balance: The new balance amount
        """
        try:
            bankAcc = BankAccount.query.filter_by(cust_id=self.cust_id).first()
            logging.info('Previous Balance is : {} '.format(bankAcc.money))
            bankAcc.money=bankAcc.money+amount
            db.session.commit()
            logging.info('New Balance is : {} '.format(bankAcc.money))
        except AttributeError:
            logging.info('Customer ID: \'{}\' doesn\'t exists. Try Again!'.format(cust_id))
        

    def withdraw(self,cust_id,amount):
        """      
        Withdraws the amount from the account.  Each withdrawal resulting in a
        negative balance also deducts a penalty fee of 5 dollars from the balance.
     
       
        Parameters:
               amount (Integer): The amount to be withdrawn
       
        Returns:
               Balance: The new balance amount
        """
        try:
           bankAcc = BankAccount.query.filter_by(cust_id=cust_id).first()
           logging.info('Previous Balance is : {} '.format(bankAcc.money))

           if bankAcc.money - amount < 0:
              logging.warn('The account balance is less than zero')
              bankAcc.money -= (amount + BankAccount.penalty_amount)
              bankAcc.money += BankAccount.penalty_amount

           else:
              bankAcc.money -= amount
              logging.info('New Balance is : {} '.format(bankAcc.money))
        except AttributeError:
              logging.info('Customer ID: \'{}\' doesn\'t exists. Try Again!'.format(cust_id))
        db.session.commit()


class CreditCard(db.Model):
    
    __tablename__ = "creditcard"
    
    title=db.Column(db.String(10),nullable='False')
    first_name=db.Column(db.String(50),nullable='False')
    last_name=db.Column(db.String(50),nullable='False')
    address=db.Column(db.String(100),nullable='False')
    card_number = db.Column(db.Integer,primary_key=True)
    expiration_month = db.Column(db.String(5),nullable='False')
    expiration_year=db.Column(db.Integer,nullable='False')
    security_code = db.Column(db.String(5),nullable='False')
    card_type = db.Column(db.String(20),nullable='False')
    currency_code = db.Column(db.String(10),nullable='False')
    beginning_balance = db.Column(db.Float,nullable='False')
    account_balance = db.Column(db.Float,nullable='False')
    charges = db.Column(db.Float,nullable='False')
    overdraft_fees = db.Column(db.Float,nullable='False')
    payments = db.Column(db.Float,nullable='False')
    ending_balance = db.Column(db.Float,nullable='False')

    def create_beginning_balance(self, card_number,amount):
        """      
        The function to establish a begining balance for a credit card
       
        Parameters: 
                card_number (int): Credit card number of the Customer
                amount(int): The amount to create the begining balance
       
        Returns:
               None
        """

        creditCard = CreditCard.query.filter_by(card_number=self.card_number).first()
        creditCard.beginning_balance+=amount
        db.session.commit()
        creditCard.account_balance+=amount
        db.session.commit()
        creditCard.ending_balance+=amount
        db.session.commit()
    
        self.ending_balance += amount
        db.session.commit()

    def create_charges(self, card_number,amount):
    
        """      
        The function to Reduces the amount from the account balance. Each charge resulting in an over draft negative account balance
        Also deducts a fee of 25 dollars from the account balance.
       
        Parameters: 
                card_number (int): Credit card number of the Customer
                amount(int): The amount to create the begining balance
       
        Returns:
               None
        """
        creditCard = CreditCard.query.filter_by(card_number=self.card_number).first()

        creditCard.account_balance -= amount
        creditCard.ending_balance -= amount
        db.session.commit()

        if creditCard.account_balance < 0:
            creditCard.account_balance -= 25
            creditCard.ending_balance -= 25
            creditCard.overdraft_fees += 25
        db.session.commit()

    def create_payments(self, card_number,amount):
        """      
        The function to Deposits the amount into the account balance.
       
        Parameters: 
                card_number (int): Credit card number of the Customer
                amount(int): The amount to create the begining balance
       
        Returns:
               None
        """
        
        creditCard = CreditCard.query.filter_by(card_number=self.card_number).first()

        creditCard.account_balance += amount
        creditCard.ending_balance += amount
        creditCard.payments += amount
        db.session.commit()


class Employees(db.Model):
    __tablename__ = 'employees'

    emp_id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80))
    department = db.Column(db.String(120))
    title = db.Column(db.String(120)) 
      


@app.cli.command("initdb")
def reset_db():
    """Drops and Creates fresh database"""

    try:
        db.drop_all()
        db.create_all()
    except OperationalError as e:
        logging.error(getattr(e, 'message', repr(e)))
        sys.exit(1)

    print("Initialized default DB")


@app.cli.command("bootstrap")
def bootstrap_data():
    """Populates database with data and present customer with options to deposit or withdraw"""
    try:
        db.drop_all()
        db.create_all()
    except OperationalError as e:
        logging.error(getattr(e, 'message', repr(e)))
        sys.exit(1)
    
    ## Creating three account types: Chequing, Saving and Business for a Customer
    acc1 = BankAccount(
        bank_acc_no=145555555,actnType='Chequing',money=20000
    )

    acc2 = BankAccount(
        bank_acc_no=145565677,actnType='Saving',money=1000
    )

    acc3= BankAccount(
        bank_acc_no=145565667,actnType='Business',money=30000
    )
   
    db.session.add(acc1)
   
    db.session.add(acc2)

    db.session.add(acc3)
    cust = Customer(cust_id=123,name='John Kang',email='john.k@gmail.com',phone='1-233-2233')

    acc1.customer=cust
    acc2.customer=cust
    acc3.customer=cust

    db.session.add(cust)
    db.session.commit()

    acctn1 = BankAccount(
        bank_acc_no=200777773,actnType='Chequing',money=20000
    )

    acctn2 = BankAccount(
        bank_acc_no=2078888899,actnType='Saving',money=1000
    )


    db.session.add(acctn1)
   
    db.session.add(acctn2)

    cust1 = Customer(cust_id=235,name='Katherine Smith',email='kat.smith@gmail.com',phone='1-456-9078')

    acctn1.customer=cust1
    acctn2.customer=cust1

    db.session.add(cust1)
    db.session.commit()

    creditCard=CreditCard(title='Ms',first_name='Nancy',last_name='Smith',address='15 Bloor St, Toronto',card_number=14566666,expiration_month='May',expiration_year=2023,security_code='677',card_type='Visa',currency_code='CAD')
    db.session.add(creditCard)
    db.session.commit()

    
  
    emp=Employees(emp_id=123456,name='Sammy',department='Retail Applications',title='IT Analyst')
    db.session.add(emp)
    db.session.commit()
    emp1=Employees(emp_id=5678,name='Nancy',department='Capital Market',title='Teller')
    db.session.add(emp1)
    db.session.commit()
    
    choice=0
    while True:
        try:
            customer_id=int(input("Please enter customer ID, Use Cust_id=123 or 235: "))
            while choice!=1 or choice!=2:
                choice=int(input("Please enter 1-Deposit or 2-Withdraw: "))
                if choice not in [1,2]:
                   logging.error("Please enter numeric input as either 1-Deposit or 2-Withdraw")
                elif choice==1:
                    b=BankAccount(cust_id=customer_id)
                    amt=int(input("Please enter the amount to deposit: "))
                    b.deposit(cust_id=customer_id,amount=amt)
            
                elif choice==2:
                    b=BankAccount(cust_id=customer_id)
                    amt=int(input("Please enter the amount to withdraw: "))
                    b.withdraw(cust_id=customer_id,amount=amt)
        except ValueError as e:
            logging.error(getattr(e, 'message', repr(e)))
            sys.exit(1)
          

if __name__ == "__main__":
    app.run()
