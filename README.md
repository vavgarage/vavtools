# vavtools

vavtools это универсальная библиотека для решения базовых повседневных задач в аналитических командах. Среди основных функций данного пакета, можно обнаружить генерацию SQL запросов к ClickHouse, загрузка данных в S3 облако, параллелизацию apply функций для Pandas, перенос DataFrames с длиной более 1 млн. в xlsx файлы с разбиением на листы, базовая и продвинутая текстовые обработки регулярными вражениями.

> ⚠ Библиотека совсем свежая и может часто изменяться. Поэтому советуем чаще следить за апдейтами и обновлять версию.
> Параллелизация на MacOS может приводить к зависаниям (не проверялось на ARM M1-2. процессорах).

## Install

vavtools поддерживает Python 3.7+ и может быть развернута на Linux, MacOS и Windows без ограничений функционала:

```bash
$ pip install vavtools
```

## Usage

### Предобработка

#### Депунктизация

```python
>>> from vavtools import vavtools as vav
  
>>> text = 'Если вам нужен образ будущего, вообразите сапог, топчущий лицо человека – вечно.'

>>> new_text = vav.de_punc(text)

'Если вам нужен образ будущего вообразите сапог топчущий лицо человека вечно'
```


#### Дедиджитализация

```python
>>> text = 'Свобода – это возможность сказать, что 2x2=4. Если дозволено это, все остальное отсюда следует.'

>>> new_text = vav.de_digit(text)

'Свобода – это возможность сказать, что x=. Если дозволено это, все остальное отсюда следует.'
```


#### Извлечение весовых/количественных характеристик из колонки DataFrame.
Предположим, необходимо достать вес, объем или кол-во штук из названия товара, для этого достаточно вызватьследующую функцию:


```python
>>> df = vav.val_extractor(df=df, text_column='SKU_NAME', var_type='weight') # добавит к изначальному фрейму колонку с весом в гр.

>>> df = vav.val_extractor(df=df, text_column='SKU_NAME', var_type='volume') # добавит к изначальному фрейму колонку с объемом в мл.

>>> df = vav.val_extractor(df=df, text_column='SKU_NAME', var_type='pieces') # добавит к изначальному фрейму колонку с кол-вом штук

```
Где df - DataFrame, text_column - название колонки из которой будет происходить извлечение, var_type - тип размера, который будет извлекаться (weight, volume, pieces)


## Запросы к ClickHouse
Запросы осуществляются с помощью встроенной библиотеки requests.


```python

>>> query = 'SELECT TOP 10 * FROM dict.global_categories'
>>> column_names = ['id','category_name']
>>> ch_user_name = '' # ваш username для подключения к CH
>>> ch_pwd = '' # ваш пароль для подключения к CH
>>> ch_driver_path = '/home/user/...' # путьк сертификату 

>>> df = vav.get_data(query, column_names, ch_user_name, ch_pwd, ch_driver_path)

```

На выходе выдается DataFrame с данными или сообщение об ошибке.


## Загрузка в yandexcloud S3

```python

>>> file2upload = '1984.txt'
>>> s3_key_id = '' # id ключа от S3
>>> s3_key = '' # сам ключ от S3
>>> bucket = '' # имя бакета в S3
>>> s3_directory = '' # имя директории в бакете S3

>>> vav.s3_upload(file2upload, s3_key_id, s3_key, bucket, s3_directory)

```

## Параллелизация

Ускоряет вызовы apply пропорционально кол-ву ядер процессора

```python
>>> def async_preprocessing(data):
        data['name'] = data.apply(lambda x: de_punc(x['name']), axis = 1)
        data['name'] = data.apply(lambda x: de_digit(x['name']), axis = 1)
        return data
        
>>> data = vav.parallelize_dataframe(data, async_preprocessing)  
```

## Сохранение больших DataFrames в XLSX формат 
```python

>>> file_name = 'huge_report.xlsx'
>>> vav.excel_saver(df, file_name, BS = 1e+6)

```
Где BS (Batch Size) - кол-во заполненных строк на одном листе Excel (максимум 1 млн.)


## Утилиты

#### Доля пропущенных значений в DataFrame по колонкам.
```python

>>> vav.get_nan_ratio(df)

```
Возвращает DataFrame вида ColumnName | NAN-Ratio, отсортированный по убыванию доли пропусков <br>

#### Поиск определннных файлов в директории.

```python
>>> vav.files_search(director='data/years', extention='.csv')
```
Вернет списоквсех файлов, в казоной директории с указанным расширением <br>

#### Время отработки функции (Декоратор).

```python
>>> @vav.execution_time
>>> def test_func(n):
      for i in range(n):
        n+=i**2
    return n

>>> test_func(10**7)
Process time: 0.0 min, 3 sec
```
