import pyodbc

conn_str = 'Driver={SQL Server};Server=CCFFDWDB1.US.CROWNCASTLE.COM,51803;Database=AdaptiveData;UID=CCIC\\kryan;PWD=!QAZ2wsx#EDC4rfv;Integrated Security=NTLM'
conn = pyodbc.connect(conn_str)