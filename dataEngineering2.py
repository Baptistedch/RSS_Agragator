from datetime import datetime
from feedparser import parse
from kafka import KafkaProducer
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json

# Configuration de Cassandra
cassandra_host = 'localhost'
cassandra_port = '9042'
cassandra_keyspace = 'rss_data'

user_id='user_103' 
password='Arnold Schwarzenegger notre DIEU'

# On utilise PlainTextAuthProvider qui est un fournisseur d'authentification pour Apache Cassandra, en se basant sur un nom d'utilisateur et un mot de passe
# Si les deux sont reconnus par Cassandra, alors l'utilisateur peut accéder à sa base Cassandra associée, une session unique
auth_provider = PlainTextAuthProvider(username=user_id, password=password)
cluster = Cluster([cassandra_host], port=cassandra_port, auth_provider=auth_provider)

# La Keyspace est fournie par le service d'authentification des utilisateurs, qui ne nous concerne pas directement.
# Cette clé permet à un utilisateur permet d'avoir son propre cluster Cassandra et son espace
session = cluster.connect(cassandra_keyspace)

# Nous avons à disposition les fonctions de webscrapper qui ne sont pas codés, mais on fait des appels dans nos codes
WebScrapper = print()
# La ligne précédente permet de visualiser le fonctionnement, mais elle n'est pas contractuelle :)
# Hiha cowboy ! 
# Cette ville est trop petite pour nous deux, dansons Don Juan !


# -----------------------------------------------------------------------------------------------------------------------------------
# Fonctions appelées


def findLast10ArticleSummaries(user_id : str):
    """"
    Cette fonction permet à un utilisateur de récupérer les 10 derniers articles auxquels il s'est abonné
    
    Parameters : 
    user_id (str) : String correspondant à la clé d'identification de l'utilsateur
    
    Returns: 
    List : Liste des articles avec les clés feed_id, article_id, title et pubDate
    """
    
    # Requête Cassandra pour récupérer les informations des 10 derniers articles
    query = "SELECT feed_id, article_id, title, pubDate FROM %s LIMIT 10;"
    rows = session.execute(query, [user_id])

    # On convertit les données en format JSON
    data = []
    for row in rows:
        item = {
            "feed_id": row.feed_id,
            "article_id": row.article_id,
            "title": row.title,
            "pubDate": str(row.pubDate)
        }
        data.append(item)

    json_data = json.dumps(data)

    # Renvoie le document JSON des 10 derniers articles
    return json_data

def findOneArticle(article_id: str, user_id):
    """""
    L'utilisateur va récupérer les détails de l'article passé en paramètre

    Parameters : 
    article_id (str) : String correspondant à la d'identifiant de l'article

    Returns:
    dict: dictionnaire conteneant les détails de l'article s'il existe, NONE sinon

    """
    #requête pour récupérer l'article avec l'ID donné
    query = f"SELECT * FROM %s WHERE article_id = '{article_id}'"
    result = session.execute(query, [user_id]).one()
    #on regarde si la ligne existe
    if result:
        return result._asdict() #on convertit en dictionnaire
    else:
        return None


def saveArticles(articles_list: list, user_id):
    """
    Sauvegarde un document JSON contenant les informations des articles de l'utilisateur dans une base de données Cassandra unique à l'utilisateur en question 

    Parameters:
    articles_list : La liste des articles de l'utilisateur 

    Returns:
    None
    """
    
    # Création d'une table unique pour l'utilisateur 
    table_name = user_id

    # Création de la table si elle n'existe aps déjà
    session.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (feed_id text, article_id text, title text, pubDate timestamp, description text, link text, PRIMARY KEY (article_id));")

    # Insertion des articles dans la table
    for article in articles_list:
        try:
            query = f"INSERT INTO {table_name} (feed_id, article_id, title, pubDate, description, link) VALUES (%s, %s, %s, %s, %s, %s);"
            values = (article['feed_id'], article['article_id'], article['title'], article['pubDate'], article['description'], article['link'])
            session.execute(query, values)
        except:
            continue


# -----------------------------------------------------------------------------------------------------------------------------------

# Les urls sont directement envoyés par la souscription de l'application
# Liste d'URLs de journaux auquel un utilisateur est abonné
urls_subscribed = [
    'https://rss.nytimes.com/services/xml/rss/nyt/World.xml',
    'https://www.lemonde.fr/rss/en_continu.xml',
    'https://www.theguardian.com/world/rss',

]


# ---------------------------------------------------------------------------------------------------------------------------------------------
# Partie Producteur de Kafka

# Configuration de Kafka
bootstrap_servers = ['localhost:9092']
topic = 'flux_rss_topic'

# Initialisation du producteur Kafka
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)


# Récupération des 10 derniers articles de chaque flux RSS et envoi au topic Kafka
for url in urls_subscribed:
    
    # Les focntions de web scrapper nous permettent de récupérer les informations des articles et renvoie un dictionnaire avec les clés de notre modèle 
    # Cassandra et les valeurs associées

    feed = parse(url)
    for entry in feed.entries:
        data = {
            'article_id': entry.id,
            'feed_id': url,
            'title': entry.title,
            'pubDate': entry.published,
            'description': entry.description,
            'link': entry.link

        }
        
        producer.send(topic, value=data)

# ---------------------------------------------------------------------------------------------------------------------------------------------
# Partie Recepteur de Kafka

# Initialisation du consommateur Kafka
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)

# Récupération des données du topic Kafka et enregistrement dans Cassandra
for message in consumer:
    data = json.loads(message.value)
    
    saveArticles(data, user_id)



