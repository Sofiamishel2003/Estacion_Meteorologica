from kafka import KafkaProducer
import json
import time
import random
import numpy as np

class WeatherStationProducer:
    def __init__(self, bootstrap_server, topic, sensor_id='sensor1'):
        """
        Inicializa el producer de la estación meteorológica
        """
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
        self.sensor_id = sensor_id
        
        # Parámetros para distribución normal (Gaussiana)
        self.temp_mean = 25.0  # Media de temperatura en °C
        self.temp_std = 15.0   # Desviación estándar
        self.hum_mean = 60.0   # Media de humedad en %
        self.hum_std = 20.0    # Desviación estándar
        
        # Direcciones del viento
        self.wind_directions = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE']
    
    def generate_temperature(self):
        """Genera temperatura con distribución normal, limitada al rango [0, 110]"""
        temp = np.random.normal(self.temp_mean, self.temp_std)
        temp = max(0.0, min(110.0, temp))  # Limitar al rango
        return round(temp, 2)
    
    def generate_humidity(self):
        """Genera humedad con distribución normal, limitada al rango [0, 100]"""
        hum = np.random.normal(self.hum_mean, self.hum_std)
        hum = max(0, min(100, int(hum)))  # Limitar al rango y convertir a entero
        return hum
    
    def generate_wind_direction(self):
        """Genera dirección del viento aleatoriamente"""
        return random.choice(self.wind_directions)
    
    def generate_data(self):
        """Genera un conjunto completo de datos meteorológicos"""
        return {
            'temperatura': self.generate_temperature(),
            'humedad': self.generate_humidity(),
            'direccion_viento': self.generate_wind_direction(),
            'timestamp': time.time()
        }
    
    def send_data(self, data):
        """Envía datos al topic de Kafka"""
        try:
            future = self.producer.send(
                self.topic,
                key=self.sensor_id.encode('utf-8'),
                value=data
            )
            # Esperar confirmación
            record_metadata = future.get(timeout=10)
            print(f"✓ Datos enviados - Topic: {record_metadata.topic}, "
                  f"Partition: {record_metadata.partition}, "
                  f"Offset: {record_metadata.offset}")
            print(f"  Datos: {data}")
            return True
        except Exception as e:
            print(f"✗ Error al enviar datos: {e}")
            return False
    
    def run(self, interval_min=15, interval_max=30):
        """
        Ejecuta el producer continuamente
        interval_min, interval_max: rango de segundos entre envíos
        """
        print(f"Iniciando Producer en topic '{self.topic}'...")
        print(f"Servidor: {self.producer.config['bootstrap_servers']}")
        print(f"Intervalo de envío: {interval_min}-{interval_max} segundos")
        print("-" * 60)
        
        try:
            while True:
                # Generar y enviar datos
                data = self.generate_data()
                self.send_data(data)
                
                # Esperar intervalo aleatorio
                wait_time = random.randint(interval_min, interval_max)
                print(f"Esperando {wait_time} segundos hasta el próximo envío...\n")
                time.sleep(wait_time)
                
        except KeyboardInterrupt:
            print("\n\nProducer detenido por el usuario")
        finally:
            self.producer.close()
            print("Conexión cerrada")


if __name__ == "__main__":
    # Configuración
    BOOTSTRAP_SERVER = 'lab9.alumchat.lol:9092'
    TOPIC = '22049'
    SENSOR_ID = 'sensor1'
    
    # Crear y ejecutar producer
    producer = WeatherStationProducer(
        bootstrap_server=BOOTSTRAP_SERVER,
        topic=TOPIC,
        sensor_id=SENSOR_ID
    )
    
    producer.run(interval_min=15, interval_max=30)