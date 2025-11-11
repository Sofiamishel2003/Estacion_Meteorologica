from kafka import KafkaProducer, KafkaConsumer
import json
import time
import random
import numpy as np
import struct

class CompressedWeatherCodec:
    """
    Codificador/Decodificador para comprimir datos meteorológicos en 3 bytes (24 bits)
    
    Distribución de bits:
    - Temperatura: 14 bits (0-16383) -> mapear a [0.00-110.00]°C con 2 decimales
    - Humedad: 7 bits (0-127) -> mapear a [0-100]%
    - Dirección del viento: 3 bits (0-7) -> 8 direcciones
    
    Total: 14 + 7 + 3 = 24 bits = 3 bytes
    """
    
    WIND_DIRECTIONS = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE']
    
    @staticmethod
    def encode(temperatura, humedad, direccion_viento):
        """
        Codifica los datos meteorológicos en 3 bytes
        
        Args:
            temperatura (float): Temperatura en °C [0.00-110.00]
            humedad (int): Humedad en % [0-100]
            direccion_viento (str): Una de las 8 direcciones
        
        Returns:
            bytes: 3 bytes con los datos codificados
        """
        # Temperatura: convertir [0.00-110.00] a [0-11000] entero (x100)
        # Luego mapear a 14 bits [0-16383]
        temp_int = int(temperatura * 100)  # 0-11000
        temp_encoded = min(16383, temp_int)  # 14 bits máximo
        
        # Humedad: [0-100] cabe perfectamente en 7 bits [0-127]
        hum_encoded = min(127, humedad)  # 7 bits máximo
        
        # Dirección del viento: índice [0-7] cabe en 3 bits
        try:
            wind_encoded = CompressedWeatherCodec.WIND_DIRECTIONS.index(direccion_viento)
        except ValueError:
            wind_encoded = 0  # Default a 'N' si no se encuentra
        
        # Combinar los bits:
        # [14 bits temp][7 bits hum][3 bits wind] = 24 bits
        combined = (temp_encoded << 10) | (hum_encoded << 3) | wind_encoded
        
        # Convertir a 3 bytes
        byte_data = combined.to_bytes(3, byteorder='big')
        
        return byte_data
    
    @staticmethod
    def decode(byte_data):
        """
        Decodifica 3 bytes a datos meteorológicos
        
        Args:
            byte_data (bytes): 3 bytes con los datos codificados
        
        Returns:
            dict: Diccionario con temperatura, humedad y dirección del viento
        """
        # Convertir bytes a entero
        combined = int.from_bytes(byte_data, byteorder='big')
        
        # Extraer bits
        wind_encoded = combined & 0b111  # Últimos 3 bits
        hum_encoded = (combined >> 3) & 0b1111111  # Siguientes 7 bits
        temp_encoded = (combined >> 10) & 0b11111111111111  # Primeros 14 bits
        
        # Decodificar valores
        temperatura = temp_encoded / 100.0  # Regresar a decimal
        humedad = min(100, hum_encoded)  # Limitar a rango válido
        direccion_viento = CompressedWeatherCodec.WIND_DIRECTIONS[wind_encoded]
        
        return {
            'temperatura': round(temperatura, 2),
            'humedad': humedad,
            'direccion_viento': direccion_viento
        }


class CompressedWeatherProducer:
    """Producer que envía datos comprimidos en 3 bytes"""
    
    def __init__(self, bootstrap_server, topic, sensor_id='sensor1'):
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda v: v  # Enviar bytes directamente
        )
        self.topic = topic
        self.sensor_id = sensor_id
        self.temp_mean = 25.0
        self.temp_std = 15.0
        self.hum_mean = 60.0
        self.hum_std = 20.0
        self.codec = CompressedWeatherCodec()
    
    def generate_data(self):
        """Genera datos meteorológicos"""
        temp = np.random.normal(self.temp_mean, self.temp_std)
        temp = max(0.0, min(110.0, round(temp, 2)))
        
        hum = np.random.normal(self.hum_mean, self.hum_std)
        hum = max(0, min(100, int(hum)))
        
        wind = random.choice(self.codec.WIND_DIRECTIONS)
        
        return temp, hum, wind
    
    def send_data(self, temperatura, humedad, direccion_viento):
        """Codifica y envía datos en 3 bytes"""
        try:
            # Codificar datos
            encoded = self.codec.encode(temperatura, humedad, direccion_viento)
            
            # Verificar tamaño
            assert len(encoded) == 3, f"Tamaño incorrecto: {len(encoded)} bytes"
            
            # Enviar
            future = self.producer.send(
                self.topic,
                key=self.sensor_id.encode('utf-8'),
                value=encoded
            )
            record_metadata = future.get(timeout=10)
            
            print(f"✓ Datos enviados (3 bytes comprimidos)")
            print(f"  Original: T={temperatura}°C, H={humedad}%, W={direccion_viento}")
            print(f"  Bytes: {encoded.hex().upper()} ({len(encoded)} bytes)")
            print(f"  Binario: {bin(int.from_bytes(encoded, 'big'))[2:].zfill(24)}")
            
            # Verificar decodificación
            decoded = self.codec.decode(encoded)
            print(f"  Decodificado: T={decoded['temperatura']}°C, "
                  f"H={decoded['humedad']}%, W={decoded['direccion_viento']}")
            return True
        except Exception as e:
            print(f"✗ Error: {e}")
            return False
    
    def run(self, interval_min=15, interval_max=30):
        """Ejecuta el producer continuamente"""
        print(f"Producer Comprimido iniciado en topic '{self.topic}'")
        print(f"Payload máximo: 3 bytes (24 bits)")
        print("-" * 70)
        
        try:
            while True:
                temp, hum, wind = self.generate_data()
                self.send_data(temp, hum, wind)
                
                wait_time = random.randint(interval_min, interval_max)
                print(f"\nEsperando {wait_time} segundos...\n")
                time.sleep(wait_time)
        except KeyboardInterrupt:
            print("\n\nProducer detenido")
        finally:
            self.producer.close()


class CompressedWeatherConsumer:
    """Consumer que recibe y decodifica datos de 3 bytes"""
    
    def __init__(self, bootstrap_server, topic, group_id='compressed_group'):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[bootstrap_server],
            group_id=group_id,
            value_deserializer=lambda m: m,  # Recibir bytes directamente
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        self.codec = CompressedWeatherCodec()
        self.data_history = []
        
        print(f"Consumer Comprimido iniciado en topic '{topic}'")
        print("-" * 70)
    
    def process_message(self, message):
        """Decodifica y procesa mensaje de 3 bytes"""
        try:
            byte_data = message.value
            
            print(f"\n✓ Mensaje recibido")
            print(f"  Bytes: {byte_data.hex().upper()} ({len(byte_data)} bytes)")
            print(f"  Binario: {bin(int.from_bytes(byte_data, 'big'))[2:].zfill(24)}")
            
            # Decodificar
            decoded = self.codec.decode(byte_data)
            
            print(f"  Decodificado:")
            print(f"    Temperatura: {decoded['temperatura']}°C")
            print(f"    Humedad: {decoded['humedad']}%")
            print(f"    Dirección del viento: {decoded['direccion_viento']}")
            
            self.data_history.append(decoded)
            print(f"  Total de datos recibidos: {len(self.data_history)}")
            
            return decoded
        except Exception as e:
            print(f"✗ Error procesando mensaje: {e}")
            return None
    
    def consume(self):
        """Consume mensajes continuamente"""
        print("\nEscuchando por mensajes... (Ctrl+C para detener)\n")
        try:
            for message in self.consumer:
                self.process_message(message)
        except KeyboardInterrupt:
            print("\n\nConsumer detenido")
        finally:
            self.consumer.close()


if __name__ == "__main__":
    import sys
    
    BOOTSTRAP_SERVER = 'lab9.alumchat.lol:9092'
    TOPIC = '22049'
    
    if len(sys.argv) < 2:
        print("Uso: python script.py [producer|consumer]")
        sys.exit(1)
    
    mode = sys.argv[1].lower()
    
    if mode == 'producer':
        producer = CompressedWeatherProducer(BOOTSTRAP_SERVER, TOPIC)
        producer.run()
    elif mode == 'consumer':
        consumer = CompressedWeatherConsumer(BOOTSTRAP_SERVER, TOPIC)
        consumer.consume()
    else:
        print("Modo inválido. Usar 'producer' o 'consumer'")