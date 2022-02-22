from datetime import datetime, date
import time
import psycopg2


def convert_to_epoch(date):
    pattern = '%d/%m/%Y'
    epoch = int(time.mktime(time.strptime(date, pattern)))
    return epoch


def create_one_month():
    dates = ''
    if date.today().month > 10:
        dates = '26'+str(date.today().month+1)+'/'+str(date.today().year)
    else:
        dates = '26'+'/0'+str(date.today().month+1)+'/'+str(date.today().year)
    return convert_to_epoch(dates)*1000


try:
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print("Last Executed : ", dt_string)

    connection = psycopg2.connect(user="admin01",
                                  password="credencesadmin",
                                  host="104.131.97.129",
                                  port="5432",
                                  database="credences")
    cursor = connection.cursor()
    command_to_get_all_cicilan = "SELECT * FROM sma_cicilan"
    cursor.execute(command_to_get_all_cicilan)
    result = cursor.fetchall()
    listPair = []

    '''
        to get credential info from all pengajuan
    '''
    for pengajuan in result:
        # idm, idp, batas cicil, cicilan
        pair_idm_idp = []
        command_base_cicil = "SELECT cicilan FROM murabahah WHERE id_pengajuan = %s"
        cursor.execute(command_base_cicil, (pengajuan[9],))
        cicilan = cursor.fetchone()
        pair_idm_idp.append(pengajuan[1])
        pair_idm_idp.append(pengajuan[9])
        pair_idm_idp.append(pengajuan[3])
        pair_idm_idp.append(cicilan[0])
        if pair_idm_idp not in listPair:
            listPair.append(pair_idm_idp)

    for pairwise in listPair:
        command_to_count = "SELECT COUNT(*) FROM sma_cicilan WHERE idp = %s"
        cursor.execute(command_to_count, (pairwise[1], ))
        counted = cursor.fetchone()
        if counted[0] < pairwise[2]:
            command_to_recap = "SELECT * FROM sma_cicilan WHERE idp = %s ORDER BY id ASC"
            cursor.execute(command_to_recap, (pairwise[1],))
            result = cursor.fetchall()
            command_insert_new_cicilan = "INSERT INTO sma_cicilan (idm, byr_n, bts_cicil, bts_bayar, jml_tgh, status, idp, terbayar) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"
            if result[-1][6] - result[-1][10] == 0:
                data_to_insert = (result[-1][1], counted[0]+1, result[-1]
                                  [3], create_one_month(), pairwise[3], 0, result[-1][9], 0)
            if result[-1][6] - result[-1][10] > 0:
                data_to_insert = (result[-1][1], counted[0]+1, result[-1][3], create_one_month(
                ), pairwise[3] + abs(result[-1][6] - result[-1][10]), 0, result[-1][9], 0)
            if result[-1][6] - result[-1][10] < 0:

                if (abs(result[-1][6] - result[-1][10])) > pairwise[3]:
                    # dompet
                    # print("tagihan : ", result[-1][6])
                    # print("terbayar : ", result[-1][10])
                    # print("Masuk ke dompet : ", abs(
                    #     result[-1][6] - result[-1][10]))
                    # print("Lebih banyak")
                    command_to_check_registered = "SELECT id, saldo FROM sma_dompet WHERE idm = %s"
                    cursor.execute(command_to_check_registered, (pairwise[0],))
                    is_registered = cursor.fetchall()
                    # print(is_registered)

                    if len(is_registered) > 0:
                        command_to_update_saldo = "UPDATE sma_dompet SET saldo = %s WHERE idm = %s"
                        cursor.execute(command_to_update_saldo,
                                       (str(is_registered[0][1]+abs(
                                           result[-1][6] - result[-1][10])), pairwise[0]))
                        connection.commit()
                    else:
                        command_to_register_dompet = "INSERT INTO sma_dompet (idm, saldo) VALUES (%s, %s) RETURNING id"
                        cursor.execute(command_to_register_dompet, (pairwise[0], abs(
                            result[-1][6] - result[-1][10])))
                        connection.commit()

                    data_to_insert = (result[-1][1], counted[0]+1, result[-1][3], create_one_month(
                    ), pairwise[3], 0, result[-1][9], 0)
                else:
                    data_to_insert = (result[-1][1], counted[0]+1, result[-1][3], create_one_month(
                    ), pairwise[3] - abs(result[-1][6] - result[-1][10]), 0, result[-1][9], 0)
            cursor.execute(command_insert_new_cicilan, data_to_insert)
            connection.commit()


except Exception as e:
    print("Something went wrong ", e)
finally:
    if connection:
        cursor.close()
        connection.close()
