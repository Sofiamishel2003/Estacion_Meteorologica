from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from collections import deque
from datetime import datetime

class WeatherStationConsumer:
    def __init__(self, bootstrap_server, topic, group_id='weather_group'):
        """
        Inicializa el consumer de la estación meteorológica
        """
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[bootstrap_server],
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',  # Empezar desde los mensajes más recientes
            enable_auto_commit=True
        )
        
        # Almacenar datos (últimos 50 puntos)
        self.max_points = 50
        self.temperatures = deque(maxlen=self.max_points)
        self.humidities = deque(maxlen=self.max_points)
        self.wind_directions = deque(maxlen=self.max_points)
        self.timestamps = deque(maxlen=self.max_points)
        
        # Contador de direcciones del viento
        self.wind_counts = {dir: 0 for dir in ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE']}
        
        print(f"Consumer iniciado en topic '{topic}'")
        print(f"Grupo: {group_id}")
        print("-" * 60)
    
    def process_message(self, message):
        """Procesa un mensaje recibido de Kafka"""
        try:
            payload = message.value
            
            # Extraer datos
            temp = payload.get('temperatura')
            hum = payload.get('humedad')
            wind = payload.get('direccion_viento')
            timestamp = payload.get('timestamp', datetime.now().timestamp())
            
            # Almacenar datos
            self.temperatures.append(temp)
            self.humidities.append(hum)
            self.wind_directions.append(wind)
            
            date = datetime.fromtimestamp(timestamp)
            self.timestamps.append(date)
            
            # Actualizar contador de viento
            if wind in self.wind_counts:
                self.wind_counts[wind] += 1
            
            print(f"✓ Mensaje recibido en {date}:")
            print(f"  Temperatura: {temp}°C")
            print(f"  Humedad: {hum}%")
            print(f"  Dirección del viento: {wind}")
            print(f"  Total de datos: {len(self.temperatures)}\n")
            
            return True
        except Exception as e:
            print(f"✗ Error procesando mensaje: {e}")
            return False
    
    def consume_and_display(self):
        """Consume mensajes y los muestra en consola"""
        print("Escuchando por mensajes... (Ctrl+C para detener)\n")
        try:
            for message in self.consumer:
                self.process_message(message)
        except KeyboardInterrupt:
            print("\n\nConsumer detenido por el usuario")
        finally:
            self.consumer.close()
            print("Conexión cerrada")
    
    def plot_data(self):
        """Crea gráficos en tiempo real de los datos"""
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(7, 5))
        fig.suptitle('Estación Meteorológica - Telemetría en Tiempo Real', 
                     fontsize=16, fontweight='bold')
        
        def update(frame):
            # Consumir nuevo mensaje si está disponible
            try:
                message = next(self.consumer, None)
                if message:
                    self.process_message(message)
            except:
                pass
            
            # Limpiar gráficos
            ax1.clear()
            ax2.clear()
            ax3.clear()
            ax4.clear()
            
            if len(self.temperatures) > 0:
                # Gráfico 1: Temperatura vs Tiempo
                ax1.plot(list(self.temperatures), 'r-o', linewidth=2, markersize=4)
                ax1.set_title('Temperatura', fontweight='bold')
                ax1.set_xlabel('Muestra')
                ax1.set_ylabel('Temperatura (°C)')
                ax1.grid(True, alpha=0.3)
                ax1.set_ylim([0, 110])
                
                # Gráfico 2: Humedad vs Tiempo
                ax2.plot(list(self.humidities), 'b-o', linewidth=2, markersize=4)
                ax2.set_title('Humedad Relativa', fontweight='bold')
                ax2.set_xlabel('Muestra')
                ax2.set_ylabel('Humedad (%)')
                ax2.grid(True, alpha=0.3)
                ax2.set_ylim([0, 100])
                
                # Gráfico 3: Temperatura vs Humedad
                ax3.scatter(self.humidities, self.temperatures, 
                           c=range(len(self.temperatures)), cmap='viridis', s=50)
                ax3.set_title('Temperatura vs Humedad', fontweight='bold')
                ax3.set_xlabel('Humedad (%)')
                ax3.set_ylabel('Temperatura (°C)')
                ax3.grid(True, alpha=0.3)
                
                # Gráfico 4: Rosa de los vientos (distribución)
                directions = list(self.wind_counts.keys())
                counts = list(self.wind_counts.values())
                colors = plt.cm.viridis([c / max(counts) if max(counts) > 0 else 0 for c in counts])
                bars = ax4.bar(directions, counts, color=colors)
                ax4.set_title('Distribución de Dirección del Viento', fontweight='bold')
                ax4.set_xlabel('Dirección')
                ax4.set_ylabel('Frecuencia')
                ax4.grid(True, alpha=0.3, axis='y')
                
                # Añadir valores encima de las barras
                for bar in bars:
                    height = bar.get_height()
                    if height > 0:
                        ax4.text(bar.get_x() + bar.get_width()/2., height,
                                f'{int(height)}',
                                ha='center', va='bottom', fontsize=9)
            
            plt.tight_layout()
        
        # Animar gráficos
        ani = FuncAnimation(fig, update, interval=1000, cache_frame_data=False)
        plt.show()


if __name__ == "__main__":
    # Configuración
    BOOTSTRAP_SERVER = 'iot.redesuvg.cloud:9092'
    TOPIC = '22398'
    GROUP_ID = 'sensor1'
    
    # Crear consumer
    consumer = WeatherStationConsumer(
        bootstrap_server=BOOTSTRAP_SERVER,
        topic=TOPIC,
        group_id=GROUP_ID
    )
    
    # Corazón, aqui podes correr los 2 modos, el de consola o el de graficos:
    # 1. Solo consumir y mostrar en consola
    # consumer.consume_and_display()
    
    # 2. Consumir y graficar en tiempo real
    consumer.plot_data()