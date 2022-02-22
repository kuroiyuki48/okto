import psycopg2
import time
from datetime import datetime


def convert_to_epoch(date):
    pattern = '%d/%m/%Y %H:%M:%S'
    epoch = int(time.mktime(time.strptime(date, pattern)))
    return epoch


def calculate_day_epoch(dateNow, dateToCheck):
    dateToCheck = dateToCheck/1000
    dateNow = datetime.fromtimestamp(dateNow)

    dateToCheck = datetime.fromtimestamp(dateToCheck)
    return (dateToCheck-dateNow).days


try:
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print("Last Executed : ", dt_string)
    print("Today's epoch : ", convert_to_epoch(dt_string))
    print(calculate_day_epoch(convert_to_epoch(dt_string), 1625924716000))

    connection = psycopg2.connect(user="admin01",
                                  password="credencesadmin",
                                  host="104.131.97.129",
                                  port="5432",
                                  database="credences")
    cursor = connection.cursor()
    command_to_count_treshold = "SELECT COUNT (*) FROM member_koperasi WHERE posisi = %s AND status_member = %s"
    cursor.execute(command_to_count_treshold, ("Investor", "Aktif"))
    n_investor = cursor.fetchall()
    treshold_investor = int(2/3 * n_investor[0][0])

    command_to_check_pengajuan = "SELECT id_pengajuan, tgl_aju FROM murabahah WHERE status_aju = %s"
    cursor.execute(command_to_check_pengajuan, (1,))
    result = cursor.fetchall()

    if len(result) > 0:
        for idp in result:
            command_check_treshold = "SELECT COUNT(*) FROM sma_validasi_invest WHERE id_pengajuan = %s AND status_aju = %s"
            cursor.execute(command_check_treshold, (idp[0], 1))
            investor_acc = cursor.fetchall()
            if investor_acc[0][0] < treshold_investor and calculate_day_epoch(convert_to_epoch(dt_string), idp[1]) < -7:
                command_update_status = "UPDATE murabahah SET status_aju = %s WHERE id_pengajuan = %s"
                cursor.execute(command_update_status, (99, idp[0]))
                connection.commit()
    else:
        print("Not record(s) found")

except Exception as e:
    print("Something went wrong ", e)

finally:
    if connection:
        cursor.close()
        connection.close()
