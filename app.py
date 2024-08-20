
import warnings, os.path, secrets, sys
from csv import writer
import pandas as pd
import numpy as np
from flask import Flask, render_template, request, send_file, flash, redirect, url_for, session,send_from_directory, url_for
import os, pyodbc, struct, csv, re
from azure import identity
from sqlalchemy import create_engine, inspect, Table, MetaData
# from flask_apscheduler import APScheduler
from Processor import process_data
import General_Extractor
import Life_Extractor
import datetime
from datetime import datetime, timedelta
from typing import Union
from dotenv import load_dotenv
from flask_sqlalchemy import SQLAlchemy
from config import Config
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from flask_bcrypt import Bcrypt
from urllib.parse import quote_plus
from functools import wraps
from urllib.parse import quote_plus
from sqlalchemy.orm import sessionmaker



load_dotenv() # specify the path to the .env file

app = Flask(__name__)
app.config.from_object("config.ProductionConfig")
print(f"THE APP IS RUNNING THE {app.config['ENV']} ENVIRONMENT")
BADGE = app.config['BADGE']
general_ratio = app.config['TABLE1']
life_ratio = app.config['TABLE2']
combined_rating = app.config['TABLE3']
general_data = app.config['TABLE4']
life_data = app.config['TABLE5']

table_names = {'general_ratio': general_ratio, 'life_ratio': life_ratio, 'combined_rating': combined_rating,
          'general_data': general_data, 'life_data': life_data}



ALLOWED_EXTENSIONS = {'txt', 'pdf', 'csv', 'xlsx','xlsm','xls' }


#Obtain database information and for an engine using .env
app.secret_key = os.environ.get('SECRET_KEY')

ODBC_Driver = os.environ.get("ODBC_Driver")
Server=os.environ.get("Server")
Database=os.environ.get("Database")
Uid=os.environ.get("Uid")
Pwd=os.environ.get("Pwd")



connection_string = f'DRIVER={ODBC_Driver};SERVER={Server};DATABASE={Database};UID={Uid};PWD={Pwd}'
encoded_connection_string = quote_plus(connection_string)

# Create the SQLAlchemy engine
# engine = create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string}')
# engine = create_engine(f'mssql+pymssql://{Uid}:{Pwd}@{Server}/{Database}')
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={encoded_connection_string}')

app.config['SQLALCHEMY_DATABASE_URI'] = f"mssql+pyodbc:///?odbc_connect={encoded_connection_string}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

bcrypt = Bcrypt(app)


def table_list():
    metadata = MetaData()
    metadata.reflect(bind=engine)
    return list(metadata.tables.keys())

def table_exists(table_name):
    metadata = MetaData()
    metadata.reflect(bind=engine)
    return table_name in metadata.tables.keys()
    




APP_FILES_PATH = "App_Files" #os.environ.get('APP_FILES_PATH')

# Initialize SQLAlchemy
db = SQLAlchemy(app)





class User(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(128))
    first_login = db.Column(db.Boolean, default=True)  # Automatically set first_login to True on creation

    def set_password(self, password):
        self.password_hash = bcrypt.generate_password_hash(password).decode('utf-8')

    def check_password(self, password):
        return bcrypt.check_password_hash(self.password_hash, password)



class UserActivity(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(255))
    page_accessed = db.Column(db.String(100))
    login_timestamp = db.Column(db.DateTime)
    leaving_timestamp = db.Column(db.DateTime)
    Narration = db.Column(db.Text, nullable=True)
    Company_Name = db.Column(db.String(255), nullable=True)
    Recommendation = db.Column(db.String(255), nullable=True)
    qualitative_year = db.Column(db.Integer, nullable=True)

    def __repr__(self):
        return f'<UserActivity {self.username} visited {self.page_accessed}>'



def log_user_activity(_func=None, *, extra_info=False):
    def decorator(view_function):
        @wraps(view_function)
        def wrapper(*args, **kwargs):
            if current_user.is_authenticated:
                activity_data = {
                    'username': current_user.username,
                    'page_accessed': request.path,
                    'login_timestamp': datetime.utcnow(),
                    'leaving_timestamp': datetime.utcnow()
                }
                
                if extra_info:
                    # Extract data directly from request.form
                    activity_data.update({
                        'Narration': request.form.get('text', ''),
                        'Company_Name': request.form.get('companyname', ''),
                        'Recommendation': request.form.get('recom', ''),
                        'qualitative_year': kwargs.get('qualitative_year', 0)
                    })
                
                activity = UserActivity(**activity_data)
                db.session.add(activity)
                db.session.commit()
            
            return view_function(*args, **kwargs)
        return wrapper

    if _func is None:
        return decorator  # Return the decorator itself if called with arguments
    else:
        return decorator(_func)  # Return the wrapped function if not called with arguments



@app.before_request
def create_tables():
    # The following line will remove this handler, making it
    # only run on the first request
    app.before_request_funcs[None].remove(create_tables)

    db.create_all()


@app.before_request
def make_session_permanent():
    """Make sessions permanent so that session lifetime is controlled by PERMANENT_SESSION_LIFETIME."""
    session.permanent = True


# # To configure session timeout duration
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(minutes=60)



login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_the_year(file,dictionary_sheets):
    file = pd.ExcelFile(file[0])
    worksheets = file.sheet_names
    
    def contains_all_keywords(data,keywords):
        return all(keyword.lower() in " ".join(map(str,data.stack().values.tolist())).lower() for keyword in keywords)
    bs = [sheet for sheet in worksheets if 'Content' not in sheet
        if contains_all_keywords(dictionary_sheets[sheet],['general', 'balance sheet'])]
    df = dictionary_sheets[bs[0]] # select the first balance sheet worksheet under general insurance 
    a = df.stack().astype(str).tolist() #combine the data frame into a single column
    s = [i for i in a if "balance SHEET".lower() in i.lower()][0] # select the cell that has the name balance sheet
    return re.search(r"\b(19[7-9]\d|20[0-3]\d)\b", s)[0] # obtain the year from string. Regular expression pattern for matching years from 1970 to 2039

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Handle POST request if necessary
        pass
    if current_user.is_authenticated:
        return redirect(url_for('home'))
    else:
        return redirect(url_for('login'))




@app.route('/home', methods=["POST", "GET"])
@login_required
@log_user_activity(extra_info=False)
def home():
    if 'user_id' not in session:
        flash('Your session has expired, please log in again.')
        return redirect(url_for('login'))
    return render_template('home.html',BADGE=BADGE) # return redirect('/')



@app.route('/login', methods=["POST", "GET"])
@log_user_activity(extra_info=False)
def login():
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']
        user = User.query.filter_by(username=email).first()  # Assuming username stores the email

        # Check if details are entered
        if not email:
            flash('Please enter your email.')
            return redirect('/login')
        if not password:
            flash('Please enter your password.')
            return redirect('/login')
        if user and user.check_password(password):
            login_user(user)
            session['user_id'] = user.id  # Explicitly setting user_id in session
            if user.first_login:
                return redirect(url_for('change_password'))
            return redirect(url_for('home'))
        else:
            flash('Invalid username or password')
    return render_template('login.html')
        
@app.route('/change_password', methods=['GET', 'POST'])
@login_required
@log_user_activity(extra_info=False)
def change_password():
    if request.method == 'POST':
        password = request.form['password']
        current_user.set_password(password)
        current_user.first_login = False
        db.session.commit()
        return redirect(url_for('home'))
    return render_template('change_password.html')


@app.route('/register', methods=['GET', 'POST'])
@login_required
@log_user_activity(extra_info=False)
def register():
    if request.method == 'POST':
        username = request.form['email']  # Using email as username
        password = request.form['password']
        if User.query.filter_by(username=username).first():
            flash('Username already exists')
            return render_template('register.html')
        
        new_user = User(username=username)
        new_user.set_password(password)
        db.session.add(new_user)
        db.session.commit()
        flash('Successfully registered '+ username)
        return redirect(url_for('register'))
    
    return render_template('register.html')



@app.route('/success', methods=['GET', 'POST'])
@login_required
@log_user_activity(extra_info=False)
def success():
    output = None
    if 'user_id' not in session:
        flash('Your session has expired, please log in again.')
        return redirect(url_for('login'))
    if 'upload_button' in request.form:

            if request.method == 'POST':
                # year_present = request.files("FINAL")
                # if to upload many file:
                year = request.form['year']
                if not year:
                    flash('Please enter the year.')
                    return redirect(url_for('home'))

                year_present = request.files.getlist("FINAL")
                if not year_present:
                    flash("Please select a file.")
                    return redirect(url_for('home'))

                # Check if the file has an allowed extension
                for file in year_present:
                    if file.filename.split('.')[-1].lower() not in ALLOWED_EXTENSIONS:
                        flash(
                            'Please select a file and ensure it only has the following file types : xlsx,xlsm,xls')
                        return redirect(url_for('home'))

                    # Convert the year to an integer
                
                
                check_year = int(year)
                
                dictionary_sheets = pd.read_excel(pd.ExcelFile(year_present[0]),pd.ExcelFile(year_present[0]).sheet_names)

                data_year = int(get_the_year(year_present,dictionary_sheets))

                # Read the CSV files into pandas DataFrames
                # df = pd.read_csv('/var/www/scoringapp/services/users/NCBA_FILES/App_Files/general_data.csv')

                    # if os.path.exists('/general_data.csv'):

                file_path = os.path.join(APP_FILES_PATH, 'original_files_dont_delete','general_data.csv')
                if table_exists(general_data):
                    df = pd.read_sql_table(general_data, engine)
                elif os.path.exists(file_path):
                    df = pd.read_csv(file_path)
                else:
                    flash('The original csv does not exist, and the database is empty')
                    return redirect(url_for('home'))

                # Check if the two previous years exist in the DataFrame.
                years = sorted(df['YEARS'].unique())
                expected_year = df['YEARS'].max() + 1
                if expected_year==check_year and expected_year == data_year:
                    year_present = year_present[0]
                    output = process_data(year, year_present,dictionary_sheets,table_names)

                    flash("LOADING COMPLETE FOR " + str(check_year) + " DATA")

                else:
                    flash('YOU INDICATED THE YEAR ' + str(check_year) + ", THE FILE UPLOADED HAD THE YEAR " + str(data_year) + ", BUT THE SYSTEM EXPECTED THE YEAR " + str(expected_year))
                    return redirect(url_for('home'))

    return render_template("home.html", output=output,BADGE=BADGE)





@app.route('/qualitative/<int:qualitative_year>', methods=['GET', 'POST'])
@login_required
@log_user_activity(extra_info=True)
def qualitative(qualitative_year):
    if 'user_id' not in session:
        flash('Your session has expired, please log in again.')
        return redirect(url_for('login'))
    if 'submit_button' in request.form:
        if request.method == 'POST':
            session_qualitative_year = session.get('qualitative_year', '')

            # pick data from HTML
            Company_Name = request.form['companyname']
            Narration = request.form['text']
            Recommendation = request.form['recom']

            if not Company_Name:
                flash("Please enter a company name.")
                return redirect(url_for('qualitative'))
            if not Narration:
                flash("Please enter a narration.")
                return redirect(url_for('qualitative'))
            if not Recommendation:
                flash("Please enter a recommendation.")
                return redirect(url_for('qualitative'))

            # Define the header list
            header = ['COMPANY', 'COMMENT', 'RECOMMENDATION', 'YEARS']



            # Append the data to a list
            data = [Company_Name, Narration, Recommendation, qualitative_year]
            qualitative_df = pd.DataFrame([data],columns=header)


            # if table exists in the database
            if table_exists(combined_rating):

                def company_renamer(x):
                    x = x.str.replace("_", " ")
                    return x

                qualitative_df["COMPANY"] = company_renamer(qualitative_df["COMPANY"])
                # Convert 'YEARS' column to numeric
                qualitative_df['YEARS'] = pd.to_numeric(qualitative_df['YEARS'], errors='coerce')
                qualitative_df['COMPANY'] = qualitative_df['COMPANY'].astype(str)

                # df_combined = pd.read_csv(combined_file_path)
                df_combined = pd.read_sql_table(combined_rating, engine)


                # Convert 'YEARS' column to numeric
                df_combined['YEARS'] = pd.to_numeric(df_combined['YEARS'], errors='coerce')
                df_combined['NAME'] = df_combined['NAME'].astype(str)

                # Match the relevant columns from qualitative data and combined data
                df_combined['COMMENT'] = np.where(df_combined['YEARS'].astype(int) == qualitative_year,
                                                    df_combined['NAME'].map(
                                                        qualitative_df[['COMPANY', 'COMMENT']].set_index(
                                                            'COMPANY').T.to_dict('records')[0]).fillna(
                                                        df_combined['COMMENT']), df_combined['COMMENT'])

                
                df_combined['RECOMMENDATION'] = np.where(df_combined['YEARS'].astype(int) == qualitative_year,
                                                            df_combined['NAME'].map(
                                                                qualitative_df[['COMPANY', 'RECOMMENDATION']].set_index(
                                                                    'COMPANY').T.to_dict('records')[0]).fillna(
                                                                df_combined['RECOMMENDATION']),
                                                            df_combined['RECOMMENDATION'])

                adjustment_mapping = {
                    'Downgraded by 1': 1,
                    'Downgraded by 2': 2,
                    'Downgraded by 3': 3,
                    'Retained': 0,
                    'Upgraded by 1': -1,
                    'Upgraded by 2': -2,
                    'Upgraded by 3': -3,
                }
                # Update 'ADJUSTMENT' column based on the 'RECOMMENDATION' values
                df_combined['ADJUSTMENT'] = df_combined['RECOMMENDATION'].map(adjustment_mapping)

                df_combined['ADJUSTMENT'] = df_combined['ADJUSTMENT'].fillna(0)
                df_combined['GRADE'] = df_combined['GRADE'].astype(float)
                # df_combined['ADJUSTMENT'] = df_combined['ADJUSTMENT'].astype(float)
                df_combined['ADJUSTMENT'] = pd.to_numeric(df_combined['ADJUSTMENT'], errors='coerce')

                df_combined['ADJUSTED_GRADE'] = np.where(df_combined['GRADE'].isnull(),np.nan,df_combined['GRADE'].fillna(0) + df_combined['ADJUSTMENT'].fillna(0))
                df_combined['ADJUSTED_RATING'] = np.where(df_combined['ADJUSTED_GRADE'].isnull(),np.nan,
                                                np.where(df_combined['ADJUSTED_GRADE'] >=4,'D',
                                                            np.where(df_combined['ADJUSTED_GRADE'] == 3,'C',
                                                                    np.where(df_combined['ADJUSTED_GRADE'] ==2,'B',
                                                                            np.where(df_combined['ADJUSTED_GRADE'] <=1,'A',np.nan)))))
                                        
                df_combined = df_combined.sort_values(['NAME', 'YEARS'])
                df_combined['ADJUSTED_RATING_Yr_1'] = df_combined.groupby(['NAME'])[\
                    'ADJUSTED_RATING'].shift(1)
                
                df_combined = df_combined.sort_values(["YEARS",'ADJUSTED_RATING','ADJUSTED_RATING_Yr_1','NAME'],
                            ascending = [False,True,True,True]).reset_index().drop('index',axis=1).reset_index()


                df_combined['SORT'] = df_combined['index'].rank()
                df_combined = df_combined.drop(['index'], axis =1)

                # Overwrite the combined file with the updated data
                # df_combined.to_csv(combined_file_path, index=False)
                # df_combined.to_csv(file_path, index=False)
                df_combined.to_sql(combined_rating,engine, index=False, if_exists="replace")



                flash(Company_Name + " details have been saved")
            else:
                flash('Upload a file')
                return redirect(url_for('home'))

    return render_template("qualitative.html", qualitative_year=qualitative_year,BADGE=BADGE)




@app.route('/proceed', methods=["POST", "GET"])
@login_required
@log_user_activity(extra_info=False)
def proceed():
    # FOR THE NAME SUGGESTIONS
    COMP = []
    names = []
    if 'user_id' not in session:
        flash('Your session has expired, please log in again.')
        return redirect(url_for('login'))
    if request.method == 'POST':
        qualitative_year = request.form['qualitative_year']
        q = qualitative_year
        if not qualitative_year:
            flash('Please enter the year.')
            return redirect(url_for('proceed'))

        # Check if the stored year is different from the entered year
        stored_year = session.get('qualitative_year')
        if stored_year and stored_year != qualitative_year:
       
            qualitative_year = session['qualitative_year']

        qualitative_year = q
        # dfc3 = 'combined_general_life_rating.csv' # +pk
        filename = 'combined_general_life_rating.csv'
        file_path = os.path.join(APP_FILES_PATH, filename) # +pk

        # dfc2 = pd.read_csv(dfc3)  # Read the CSV file into a DataFrame
        dfc2 =  pd.read_sql_table(combined_rating, engine)
        
        int_year = dfc2.loc[:, 'YEARS'].astype(int)
        years = sorted(set(int_year))

        if int(qualitative_year) not in years:
            flash("Please ensure you have uploaded a file for analysis.")
            return redirect(url_for('home'))

        # filename = '/var/www/scoringapp/services/users/NCBA_FILES/App_Files/combined_general_life_rating.csv'
        # filename = 'combined_general_life_rating.csv'
        # file_path = os.path.join(APP_FILES_PATH, filename)

        df = pd.read_sql_table(combined_rating, engine)
        # Filter the DataFrame to include only the company names of the selected year
        df = df[df['YEARS'].astype(int) == int(qualitative_year)]
        names = df["NAME"].tolist()
        COMP = [str(i).replace(" ", "_") for i in names]
        COMP = sorted(COMP)
        session['COMP'] = COMP

        # Store the 'qualitative_year' value in the session
        session['qualitative_year'] = qualitative_year

        return render_template("qualitative.html", COMPANIES=COMP, qualitative_year=qualitative_year,BADGE=BADGE)

    return render_template("proceed.html",BADGE=BADGE)
    



@app.route('/feedback', methods=["POST", "GET"])
@login_required
@log_user_activity(extra_info=False)
def feedback():
    if 'user_id' not in session:
        flash('Your session has expired, please log in again.')
        return redirect(url_for('login'))

    if request.method == 'POST':
        subject = request.form.get('subject')
        main_message = request.form.get('message')
        if not subject or not main_message:
            flash('Please fill out all fields.')
            return render_template("feedback.html", BADGE=BADGE)
        redirect_url = f'mailto:pmaloba@strathmore.edu?subject={quote_plus(subject)}&body={quote_plus(main_message)}'
        return redirect(redirect_url)
    
    return render_template("feedback.html", BADGE=BADGE)






@app.route('/logout', methods=["GET"])
@log_user_activity(extra_info=False)
def logout():
   
    session.clear()
    logout_user()
    return redirect('/login')
   


if __name__ == '__main__':
    app.run()
    # app.run(host='0.0.0.0', port = 5000,debug=True )
    # app.run(host='0.0.0.0', port = 8000,debug=True )
    # app.run(host='0.0.0.0', port = 8000)
