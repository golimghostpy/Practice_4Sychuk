import time
import requests
import json
import random
import threading

lock = threading.Lock() # не даем двум ботам одновременного доступа к одной функции
BASE_URL = "http://localhost:5000"  # Замените на адрес вашего Flask-приложения

def create_user(username): # создание пользователя
    with lock:
        response = requests.post(f"{BASE_URL}/user", json={"username": username})
        if response.status_code == 200:
            return response.json().get("key")
        return

def get_user_id(userKey): # получение id по ключу
    with lock:
        headers = {'X-USER-KEY': userKey}
        response = requests.get(f"{BASE_URL}/user_id", headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            return

def create_order(user_key, pair_id, quantity, price, orderType, keyFilter): # размещение ордера
    with lock:
        headers = {'X-USER-KEY': user_key}
        orderData = {
            "pair_id": pair_id,
            "quantity": quantity,
            "price": price,
            "type": orderType,
            "user_filter": keyFilter
        }

        try:
            response = requests.post(f"{BASE_URL}/order", json=orderData, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                return
        except Exception as e:
            print(f"An error occurred: {e}")

def get_orders(): # получен ордеров
    with lock:
        response = requests.get(f"{BASE_URL}/order")
        if response.status_code == 200:
            return response.json()
        return

def get_balance(userKey): # получение баланса пользователя
    with lock:
        headers = {'X-USER-KEY': userKey}
        response = requests.get(f"{BASE_URL}/balance", headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            return

def get_lot(): # получение списка лотов
    with lock:
        response = requests.get(f"{BASE_URL}/lot")
        return response.json()

def get_pair(): # получение списка пар
    with lock:
        response = requests.get(f"{BASE_URL}/pair")
        return response.json()

def greedy_bot(botId): # жадный бот (фарм рубля)
    greedyName = "greedy_bot" + botId
    userKey = create_user(greedyName)
    time.sleep(1)

    if userKey: # если уже запускали бота
        with open(f"{greedyName}.txt", "w") as keyFile:
            keyFile.write(userKey)
    else:
        with open(f"{greedyName}.txt", "r") as keyFile:
            userKey = keyFile.readline().strip()

    randomKey = ''
    while randomKey == '': # ожидание активации авто-бота
        with open(f"random_bot{botId}.txt", "r") as keyFile:
            randomKey = keyFile.readline().strip()

    pairs = get_pair()

    balance = get_balance(userKey)

    userBalance = {}
    for item in balance: # получаем баланс
        userBalance[int(item['lot_id'])] = float(item['quantity'])

    rub_lot_id = next((i['lot_id'] for i in get_lot() if i['name'] == 'RUB')) # определяем id рубля

    randomId = get_user_id(randomKey) # получаем id авто-бота, связанного с нами

    while True:
        orders = get_orders()
        if not orders:
            continue
        time.sleep(1)
        for lot_id, quantity in userBalance.items():
            if lot_id == rub_lot_id or quantity <= 0:
                continue

            pair_info = next((pair for pair in pairs if pair['sale_lot_id'] == rub_lot_id and pair['buy_lot_id'] == lot_id), None)

            if pair_info:
                pair_id = pair_info['pair_id']

                # фильтруем ордера, чтобы они были от авто=бота с таким же id
                sell_orders = [order for order in orders if order['closed'] == '-' and order['user_id'] == randomId and order['type'] == 'sell' and order['pair_id'] == pair_id]
                sell_orders.sort(key=lambda x: x['price']) # нам нужны самые дешевые предложения

                for order in sell_orders:
                    price = order['price']
                    quantity = order['quantity']

                    buyQuantity = userBalance[lot_id] // price if userBalance[lot_id] // price <= quantity else quantity # считаем, сколько мы можем купить за эту цену
                    # Размещаем заказ на покупку
                    create_order(userKey, order['pair_id'], buyQuantity, price, 'buy', randomKey) # покупаем
                    print(f"{botId} bought {buyQuantity} at price {price} from order {order['order_id']}")
                    userBalance[lot_id] -= buyQuantity * price # запоминаем, сколько потратили, чтобы постоянно не перегружать баланс
                    time.sleep(1)
        time.sleep(1)

def random_bot(botId):
    randomName = "random_bot" + botId
    userKey = create_user(randomName)

    if userKey:
        with open(f"{randomName}.txt", "w") as keyFile:
            keyFile.write(userKey)
    else:
        with open(f"{randomName}.txt", "r") as keyFile:
            userKey = keyFile.readline().strip()

    time.sleep(1)
    with open(f"greedy_bot{botId}.txt", "r") as keyFile:
        greedyKey = keyFile.readline().strip()

    while True:
        pairId = random.randint(1, 21)
        quantity = random.randint(10, 500)
        price = random.uniform(0.01, 0.5)
        if 1 <= pairId <= 6:
            orderType = 'sell'
        else:
            orderType = random.choice(['buy', 'sell'])

        create_order(userKey, pairId, quantity, price, orderType, greedyKey)
        time.sleep(1)


if __name__ == "__main__":
    threads = []
    cntThreads = int(input("Enter count of post bots: "))

    for i in range(cntThreads):
        with open(f"random_bot{i}.txt", "w") as keyFile:
            keyFile.write("")
        with open(f"greedy_bot{i}.txt", "w") as keyFile:
            keyFile.write("")

    for i in range(cntThreads): # выделяем каждого бота в отдельный поток для параллельного выполнения
        threads.append(threading.Thread(target=random_bot, args=(str(i))))
        threads[-1].start()
        threads.append(threading.Thread(target=greedy_bot, args=(str(i))))
        threads[-1].start()
        time.sleep(1)