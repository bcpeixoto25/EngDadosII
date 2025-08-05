from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import desafio


class WatcherHandler(FileSystemEventHandler):
    def fluxo(self, src_path):
        df = desafio.csv_bronze(src_path)
        df_modificado = desafio.modificar(df)
        if desafio.verifica_df(df_modificado):
            desafio.create_schema_silver()
            desafio.create_silver_tab_desafio(df_modificado)
        else:
            print('Verifique fluxo! Não deu certo!')

    def on_modified(self, event):
        print(f"Arquivo modificado: {event.src_path}")
        extensao = event.src_path.split('.')[-1]
        print(f'A extensão é {extensao}')
        if extensao == 'csv':
            print(f"Iniciando ingestão de dados com o arquivo:{event.src_path}")
            self.fluxo(event.src_path)
        else:
            print('Nada a fazer. O arquivo modificado não é .csv')   

    def on_created(self, event):
        print(f"Arquivo criado: {event.src_path}")
        extensao = event.src_path.split('.')[-1]
        print(f'A extensão é {extensao}')
        if extensao == 'csv':
            print(f"Iniciando ingestão de dados com o arquivo: {event.src_path}")
            self.fluxo(event.src_path)
        else:
            print('Nada a fazer. O arquivo modificado não é .csv')

    def on_deleted(self, event):
        print(f"Arquivo deletado: {event.src_path}")
        print('Nada a fazer')


if __name__ == "__main__":
    path_to_watch = ".\pasta"  # Directory to monitor
    event_handler = WatcherHandler()
    observer = Observer()
    observer.schedule(event_handler, path=path_to_watch, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
