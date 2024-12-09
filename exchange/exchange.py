import threading
import time
from flask import Flask, request, jsonify
from random import randint
import uuid
import socket
import json

lots = []
lock = threading.Lock()

app = Flask(__name__) # создаем приложение для прослушивания команд
clientSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # клиентский сокет

def send_to_server(message): # отправка сообщения на сервер и возврат ответа
    with lock: # блокируем несколько одновременных запросов
        try:
            clientSock.sendall(message.encode())
            time.sleep(0.1) # даем время обработать запрос
            response = clientSock.recv(1024)
            return response.decode()
        except Exception as e:
            print(f"Error communicating with server: {e}")

def low_balance(userId, lotId, lowOn): # уменьшить баланс какого-то лота
    quantity = float(send_to_server(f"SELECT user_lot.quantity FROM user_lot WHERE user_lot.user_id = '{userId}' AND user_lot.lot_id = '{lotId}'").split('\n')[1].strip(';'))
    if quantity < lowOn:
        return False
    quantity -= lowOn
    send_to_server(f"DELETE FROM user_lot WHERE user_lot.user_id = '{userId}' AND user_lot.lot_id = '{lotId}'")
    send_to_server(f"INSERT INTO user_lot VALUES ('{userId}', '{lotId}', '{float(quantity)}')")
    return True

def up_balance(userId, lotId, upOn): # увеличить баланс
    quantity = float(send_to_server(f"SELECT user_lot.quantity FROM user_lot WHERE user_lot.user_id = '{userId}' AND user_lot.lot_id = '{lotId}'").split('\n')[1].strip(';'))
    quantity += upOn
    send_to_server(f"DELETE FROM user_lot WHERE user_lot.user_id = '{userId}' AND user_lot.lot_id = '{lotId}'")
    send_to_server(f"INSERT INTO user_lot VALUES ('{userId}', '{lotId}', '{float(quantity)}')")

@app.route('/user', methods=['POST'])
def create_user(): # создание пользователя
    data = request.get_json()
    username = data.get('username')

    takenUsernames = send_to_server(f"SELECT user.username FROM user WHERE user.username = '{username}'").split("\n")[1:-2]
    if len(takenUsernames) != 0: # если пользователь с таким именем уже существует
        return jsonify({"error": "Username is already exists"})

    while True:
        # Генерация уникального ключа
        key = str(uuid.uuid4()).replace('-', '')
        keyCheck = send_to_server(f"SELECT user.key FROM user WHERE user.key = '{key}'").split("\n")[1:-2]
        if len(keyCheck) == 0:
            break

    send_to_server(f"INSERT INTO user VALUES ('{username}', '{key}')") # вставка пользователя в бд

    userId = send_to_server(f"SELECT user.user_id FROM user WHERE user.username = '{username}'").split('\n')[1] # получаем ID пользователя
    for i in range(1, len(lots) + 1): # пополняем ему кошелек
        send_to_server(f"INSERT INTO user_lot VALUES ('{userId[:-1]}', '{i}', '1000')")

    return jsonify({"key": key}) # отправляем пользователю его ключ

def change_status(orderId, userId, pairId, quantity, price, type, closeKey): # изменить состояние ордера с закрытого
    with open('../bin/trader/order/order_pk_sequence.txt', 'r') as pkFile: # запоминаем текущий максимальный ID
        tempId = int(pkFile.readline().strip())

    send_to_server(f"DELETE FROM order WHERE order.order_id = '{orderId}'") # удаляем строку с нашим ордером

    with open('../bin/trader/order/order_pk_sequence.txt', 'w') as pkFile: # записываем нужный ID в файл
        pkFile.write(str(orderId))

    # вставляем ордер с нужными значениями и таким же ID
    send_to_server(f"INSERT INTO order VALUES ('{userId}', '{pairId}', '{float(quantity)}', '{price}', '{type}', '{closeKey}')")

    with open('../bin/trader/order/order_pk_sequence.txt', 'w') as pkFile: # возвращаем старый максимальный ID
        pkFile.write(str(tempId))

def gen_close_key(): # генерация ключей дл закрытия сделки
    while True:
        key = randint(1000000000, 9999999999)
        checkKey = send_to_server(f"SELECT order.closed FROM order WHERE order.closed = '{key}'").split('\n')[1:-2]
        if len(checkKey) == 0: # проверка уникальности
            return key

@app.route('/user_id', methods=['GET'])
def get_user_id(): # получение id по ключу
    userKey = request.headers.get('X-USER-KEY')

    if not userKey:
        return jsonify({"error": "You must enter your key"}), 400

    try:
        return send_to_server(f"SELECT user.user_id FROM user WHERE user.key = '{userKey}'").split("\n")[1].strip(';')
    except Exception:
        return jsonify({"error": "No such a key"}), 400

def what_to_order(data, userId, firstLot, secondLot, oppositeType, userFilter): # обработка покупки и продажи
    # сразу снимаем средства с баланса при создании ордера
    if data['type'] == 'buy':
        if not low_balance(userId, secondLot, data['quantity'] * data['price']):
            return 'Not enough funds to place order'
    else:
        if not low_balance(userId, firstLot, data['quantity']):
            return 'Not enough funds to place order'

    idFilter = send_to_server(f"SELECT user.user_id FROM user WHERE user.key = '{userFilter}'").split('\n')[1][:-1]

    # получаем все ордеры с подходящим условием, но противоположным типом
    temp = send_to_server(f"SELECT order.order_id, order.user_id, order.quantity, order.price FROM order WHERE order.pair_id = '{data['pair_id']}' AND order.type = '{oppositeType}' AND order.closed = '-' AND order.user_id = '{idFilter}'").split('\n')[1:-2]

    if len(temp) == 0:
        # если нет таких ордеров, то просто вставляем в таблицу ордер и отправляем ID пользователю
        send_to_server(f"INSERT INTO order VALUES ('{userId}', '{data['pair_id']}', '{float(data['quantity'])}', '{data['price']}', '{data['type']}', '-')")

        with open('../bin/trader/order/order_pk_sequence.txt', 'r') as pkFile:
            orderId = int(pkFile.readline().strip()) - 1

        return orderId

    # выбираем ордеры с подходящей ценой
    orderMatrix = []
    if data['type'] == 'buy':
        for i in temp:
            if (float(i.split(';')[3]) <= data['price'] and i.split(';')[1] != userId):
                oneOrder = []
                for j in i.split(';')[:-1]:
                    oneOrder.append(j)
                orderMatrix.append(oneOrder)
    else:
        for i in temp:
            if (float(i.split(';')[3]) >= data['price'] and i.split(';')[1] != userId):
                oneOrder = []
                for j in i.split(';')[:-1]:
                    oneOrder.append(j)
                orderMatrix.append(oneOrder)

    # сортируем по цене
    if data['type'] == 'buy':
        orderMatrix.sort(key=lambda x: float(x[3]))
    else:
        orderMatrix.sort(key=lambda x: float(x[3]), reverse=True)

    returnId = 0
    for order in orderMatrix: # проходим по всем найденным запросам
        # если текущий запрос превышает найденный по количеству
        if data['quantity'] > float(order[2]):
            if data['type'] == 'buy':
                data['quantity'] -= float(order[2])
                up_balance(userId, firstLot, float(order[2]))
                up_balance(userId, secondLot, float(order[2]) * (data['price'] - float(order[3])))
                up_balance(order[1], secondLot, float(order[2]) * float(order[3]))

                closeKey = gen_close_key()

                change_status(order[0], order[1], data['pair_id'], order[2], order[3], oppositeType, closeKey)
                send_to_server(f"INSERT INTO order VALUES ('{userId}', '{data['pair_id']}', '{float(order[2])}', '{order[3]}', '{data['type']}', '{closeKey}')")
                send_to_server(f"INSERT INTO order VALUES ('{userId}', '{data['pair_id']}', '{float(data['quantity'])}', '{data['price']}', '{data['type']}', '-')")

                with open('../bin/trader/order/order_pk_sequence.txt', 'r') as pkFile:
                    returnId = int(pkFile.readline().strip()) - 1
            else:
                up_balance(userId, secondLot, float(order[2]) * data['price'])
                up_balance(order[1], firstLot, float(order[2]) * float(data['price']))
                up_balance(order[1], secondLot, float(order[2]) * (float(order[3]) - data['price']))
                data['quantity'] -= float(order[2])

                closeKey = gen_close_key()

                change_status(order[0], order[1], data['pair_id'], order[2], data['price'], oppositeType, closeKey)
                send_to_server(f"INSERT INTO order VALUES ('{userId}', '{data['pair_id']}', '{float(order[2])}', '{data['price']}', '{data['type']}', '{closeKey}')")
                send_to_server(f"INSERT INTO order VALUES ('{userId}', '{data['pair_id']}', '{float(data['quantity'])}', '{data['price']}', '{data['type']}', '-')")

                with open('../bin/trader/order/order_pk_sequence.txt', 'r') as pkFile:
                    returnId = int(pkFile.readline().strip()) - 1
        # если найденный запрос полностью покрывает текущий
        elif data['quantity'] < float(order[2]):
            if data['type'] == 'buy':
                up_balance(userId, firstLot, data['quantity'])
                up_balance(userId, secondLot, data['quantity'] * (data['price'] - float(order[3])))
                up_balance(order[1], secondLot, (float(order[2]) - data['quantity']) * float(order[3]))

                closeKey = gen_close_key()

                send_to_server(f"INSERT INTO order VALUES ('{userId}', '{data['pair_id']}', '{float(data['quantity'])}', '{order[3]}', '{data['type']}', '{closeKey}')")

                with open('../bin/trader/order/order_pk_sequence.txt', 'r') as pkFile:
                    returnId = int(pkFile.readline().strip()) - 1

                change_status(order[0], order[1], data['pair_id'], data['quantity'], order[3], f'{oppositeType}', closeKey)
                send_to_server(f"INSERT INTO order VALUES ('{order[1]}', '{data['pair_id']}', '{float(order[2]) - data['quantity']}', '{order[3]}', '{oppositeType}', '-')")

            else:
                up_balance(userId, secondLot, data['quantity'] * data['price'])
                up_balance(order[1], firstLot, data['quantity'])
                up_balance(order[1], secondLot, data['quantity'] * (float(order[3]) - data['price']))

                closeKey = gen_close_key()

                change_status(order[0], order[1], data['pair_id'], data['quantity'], data['price'], oppositeType, closeKey)

                with open('../bin/trader/order/order_pk_sequence.txt', 'r') as pkFile:
                    returnId = int(pkFile.readline().strip()) - 1

                send_to_server(f"INSERT INTO order VALUES ('{userId}', '{data['pair_id']}', '{float(data['quantity'])}', '{data['price']}', '{data['type']}', '{closeKey}')")
                send_to_server(f"INSERT INTO order VALUES ('{order[1]}', '{data['pair_id']}', '{float(order[2]) - data['quantity']}', '{order[3]}', '{oppositeType}', '-')")
        # если значения совпали (то есть не надо создавать доп. запрос)
        else:
            if data['type'] == 'buy':
                up_balance(userId, firstLot, data['quantity'])
                up_balance(userId, secondLot, data['quantity'] * (data['price'] - float(order[3])))
                up_balance(order[1], secondLot, data['quantity'] * float(order[3]))
            else:
                up_balance(order[1], firstLot, data['quantity'])
                up_balance(order[1], secondLot, data['quantity'] * (data['price'] - float(order[3])))
                up_balance(userId, secondLot, data['quantity'] * float(order[3]))

            closeKey = gen_close_key()

            change_status(order[0], order[1], data['pair_id'], data['quantity'], order[3], oppositeType, closeKey)
            send_to_server(f"INSERT INTO order VALUES ('{userId}', '{data['pair_id']}', '{float(data['quantity'])}', '{order[3]}', '{data['type']}', '{closeKey}')")

            with open('../bin/trader/order/order_pk_sequence.txt', 'r') as pkFile:
                returnId = int(pkFile.readline().strip()) - 1

    return returnId

@app.route('/order', methods=['POST']) # размещение ордера
def create_order():
    data = request.get_json()
    userKey = request.headers.get('X-USER-KEY')

    if not userKey:
        return jsonify({"error": "You must enter your key"}), 400

    try:
        userId, keyCheck = send_to_server(f"SELECT user.user_id, user.key FROM user WHERE user.key = '{userKey}'").split("\n")[1].split(';')[:-1]
    except Exception:
        return jsonify({"error": "No such a key"}), 400

    # проверка всех параметров для ордера
    if 'pair_id' not in data or 'quantity' not in data or 'price' not in data or 'type' not in data:
        return jsonify({"error": "Must enter fields: pair_id, quantity, price and type"}), 400

    try:
        # получаем индексы лотов
        firstLot, secondLot = send_to_server(f"SELECT pair.first_lot_id, pair.second_lot_id FROM pair WHERE pair.pair_id = '{data['pair_id']}'").split("\n")[1].strip(';').split(';')

        if data['type'] == 'buy':
            oppositeType = 'sell'
        else:
            oppositeType = 'buy'

        returnId = str(what_to_order(data, userId, firstLot, secondLot, oppositeType, data['user_filter']))
        if returnId.isdigit():
            return jsonify({'order_id': int(returnId)}) # ID выставленного ордера
        else:
            return jsonify({'error': returnId}) # не хватило баланса для размещения
    except Exception:
        return jsonify({'error': 'error'})


@app.route('/order', methods=['GET']) # получение списка ордеров
def get_order():
    try:
        orders = send_to_server(f"SELECT order.order_id, order.user_id, order.pair_id, order.quantity, order.type, order.price, order.closed FROM order")
        response = []
        for order in orders.split("\n")[1:-2]:
            parts = order.split(";")
            response.append({
                "order_id": int(parts[0]),
                "user_id": int(parts[1]),
                "pair_id": int(parts[2]),
                "quantity": float(parts[3]),
                "type": parts[4],
                "price": float(parts[5]),
                "closed": parts[6]
            })
        return jsonify(response), 200
    except Exception as e:
        print(f"Error fetching orders: {e}")
        return jsonify({"error": "An error occurred while fetching orders."}), 500

@app.route('/order', methods=['DELETE'])
def delete_order(): # удаление ордера
    data = request.get_json()
    userKey = request.headers.get('X-USER-KEY')

    if not userKey:
        return jsonify({"error": "You must enter your key"}), 400

    try:
        userId, keyCheck = send_to_server(f"SELECT user.user_id, user.key FROM user WHERE user.key = '{userKey}'").split("\n")[1].split(';')[:-1]
    except Exception:
        return jsonify({"error": "No such a key"}), 400

    if 'order_id' not in data:
        return jsonify({'error': 'You must enter order_id to delete'}), 400

    try: # проверка наличия ордера
        orderOwner = send_to_server(f"SELECT order.user_id FROM order WHERE order.order_id = '{data['order_id']}'").split("\n")[1]
    except Exception:
        return jsonify({'error': 'No order with that id'}), 400

    if userId != orderOwner[:-1]: # проверка доступа к ордеру у пользователя
        return jsonify({'error': 'It is not your order'}), 400

    pairId, quantity, price, type, closed = send_to_server(f"SELECT order.pair_id, order.quantity, order.price, order.type, order.closed FROM order WHERE order.order_id = '{data['order_id']}'").split("\n")[1].split(';')[:-1]

    firstLot, secondLot = send_to_server(f"SELECT pair.first_lot_id, pair.second_lot_id FROM pair WHERE pair.pair_id = '{pairId}'").split("\n")[1].split(';')[:-1]

    if not closed == '-': # если ордер уже закрыт, то его нельзя удалить
        return jsonify({'error': 'You can\'t delete closed order'})

    if type == 'buy': # возвращаем средства на баланс
        up_balance(userId, secondLot, float(quantity) * float(price))
    else:
        up_balance(userId, firstLot, float(quantity))

    send_to_server(f"DELETE FROM order WHERE order.order_id = '{data['order_id']}'") # удаляем ордер из бд
    return jsonify({"message": "Order deleted successfully"}), 200

@app.route('/lot', methods=['GET']) # получаем список лотов
def get_lot():
    lots = send_to_server(f"SELECT lot.lot_id, lot.name FROM lot")

    response = []
    for order in lots.split("\n")[1:-2]:
        parts = order.split(";")
        response.append({"lot_id": int(parts[0]), "name": parts[1]})

    return jsonify(response), 200

@app.route('/pair', methods=['GET']) # получаем список пар лотов
def get_pair():
    pairs = send_to_server(f"SELECT pair.pair_id, pair.first_lot_id, pair.second_lot_id FROM pair")

    response = []
    for order in pairs.split("\n")[1:-2]:
        parts = order.split(";")
        response.append({"pair_id": int(parts[0]), "sale_lot_id": int(parts[1]), "buy_lot_id": int(parts[2])})

    return jsonify(response), 200

@app.route('/balance', methods=['GET']) # просмотр баланса
def get_balance():
    userKey = request.headers.get('X-USER-KEY')

    if not userKey:
        return jsonify({"error": "You must enter your key"}), 400

    try:
        userId = send_to_server(f"SELECT user.user_id FROM user WHERE user.key = '{userKey}'").split("\n")[1].strip(';')
    except Exception:
        return jsonify({"error": "No such a key"}), 400

    userLots = send_to_server(f"SELECT user_lot.lot_id, user_lot.quantity FROM user_lot WHERE user_lot.user_id = '{userId}'").split('\n')[1:-2]
    response = []
    for lot in userLots:
        parts = lot.split(';')
        response.append({'lot_id': parts[0], 'quantity': parts[1]})

    return jsonify(response), 200

def get_config(): # получаем информацию для биржи
    with open('config.json', 'r') as configFile:
        config = json.load(configFile)
        return [config['lots'], config['database_ip'], config['database_port']]

def add_lots(lots): # заполняем лоты
    with open('../bin/trader/lot/lot_pk_sequence.txt', 'r') as idFile:
        if (idFile.readline() != '1'):
            return

    for i in lots:
        send_to_server(f"INSERT INTO lot VALUES ('{i}')")

    for i in range(len(lots) - 1):
        for j in range(i + 1, len(lots)):
            send_to_server(f"INSERT INTO pair VALUES ('{i + 1}', '{j + 1}')")

if __name__ == '__main__':
    lots, dbIP, dbPort = get_config()

    serverAddr = (str(dbIP), int(dbPort))
    clientSock.connect(serverAddr) # подключаемся к серверу

    add_lots(lots)

    app.run(debug=True, threaded=True) # начинаем прослушивать консоль

