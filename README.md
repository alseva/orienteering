# Волонтерский проект "Расчет ранга спортсменов"
_Создан для [Федерации спортивного ориентирования Республики Карелия (ФСО РК)](http://fso.karelia.ru/)_

**work in progress**

## Результаты
* Создан документ [**Положение о ранге.pdf**](/Положение%20о%20ранге.pdf), регламентирующий порядок расчета ранга. Ключевые моменты описаны в разделе [Этапы работы](https://github.com/alseva/orienteering/tree/master#%D1%8D%D1%82%D0%B0%D0%BF%D1%8B-%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D1%8B)
* Текущий ранг публикуется на сайте [ФСО РК](http://fso.karelia.ru/) после каждого соревнования. _Пример: [мужчины](http://fso.karelia.ru/wp-content/uploads/2023/10/%D0%A1%D0%BF%D1%80%D0%B8%D0%BD%D1%82-%D1%80%D0%B0%D0%BD%D0%B3-%D0%BD%D0%B0-2023-09-21-%D0%BC%D1%83%D0%B6%D1%87%D0%B8%D0%BD%D1%8B_2023.pdf), [женщины](http://fso.karelia.ru/wp-content/uploads/2023/10/%D0%A1%D0%BF%D1%80%D0%B8%D0%BD%D1%82-%D1%80%D0%B0%D0%BD%D0%B3-%D0%BD%D0%B0-2023-09-21-%D0%B6%D0%B5%D0%BD%D1%89%D0%B8%D0%BD%D1%8B_2023.pdf)_
* Дополнительно формируется список зарегистрировавшихся участников, не пришедших на соревнование (необходим для оценки количества впустую потраченных средств - печать карт, стартовых номеров)
* Предусмотрена возможность гибкой настройки параметров формулы ранга
(_например, коэффициенты уровней и видов стартов, ранги групп, уровень штрафов за пропуски и снятия и пр._)

## Этапы работы
Работа над проектом шла в несколько этапов. Сначала был сделан MVP, выдававший математически корректный расчет ранга, отражающий описананный в [Положении о ранге](/Положение%20о%20ранге.pdf) алгоритм. 
Далее совместно с участниками от ФСО РК обсуждали формулу расчета, параметры, доступные для настройки и перенастройки. Добавляли / убирали нюансы расчетов. Перепрогоняли расчет на разных параметрах формулы, искали оптимальный вариант, сравнивая результаты.
1. [Положение о ранге](/Положение%20о%20ранге.pdf) легло в основу _"Конфигуратора формулы ранга.xlsx"_, который настраивает пользователь. Далее из файла все параметры сохраняются в классе [RankFormulaConfig](/rank_formula_config.py). 

2. Краткий алгоритм расчета ранга:
    * Исходные данные: протоколы результатов соревнований по возрастным группам. Пример: [Золотая осень 2023. Кросс - классика, 01.10.2023](http://fso.karelia.ru/wp-content/uploads/2023/09/20231001_ResultList.htm)
    * Текущий ранг спортсмена - это среднее всех (до середины сезона) / 50% лучших (во второй половине сезона) его рангов по каждому соревнованию
    * Расчет ранга спортсмена по каждому соревнованию производится на основе многих параметров. Основные: 
       * место, занятое в соревнованиях
       * время отставания от 1го места
       * возрастная группа
       * уровень спортсменов, с которыми участник соревновался (определяется текущим рангом участников)
       * итоговый ранг прошлого сезона
    * Ранг подсчитывается как отдельно по мужчинам и женщинам, так и единым протоколом. 
    * Все возрастные группы объединяются. Уровень каждой из групп определяется специальным коэффициентом (например, для Ж21 базовый ранг 100, а для Ж40 базовый ранг 70)
    * Выходнные данные:
       * файл текущего ранга общий (мужчины + женщины), отдельно по мужчинам, отдельно по женщинам **TODO: поля**
       * файл всех протоколов соревнований с рассчитанными рангами по каждому из них. **TODO: поля**
       * файл с перечнем учасников, зарегистрировавшихся, но не пришедших на соревнования, со снятыми участниками
       * файл с участниками, у которых не указан год рождения
       * **TODO: дописать**

3. Для управления самим расчетом был создан еще один документ _"Конфигуратор приложения.xlsx"_, его также заполняет пользователь. Параметры сохраняются в классе [ApplicationConfig](/app_config.py). Доступный функционал:
   * Тип ранга для расчета: _спринт, лесной, общий летний, общий зимний_
   * Вариант загрузки протоколов: _файл, ссылка_
   * Директории для сохранения результатов
   * Год сезона
   * Список ссылок на протоколы и флаги к какому типу ранга относится файл
   * Очистка данных от ошибок (маппинг ФИО, года рождения): _так как спортсмены сами регистрируются на соревнования, возможны ошибки в ФИО, отсутствие года рождения и пр._ 

4. Библиотека Pandas использовалась как основной инструмент для работы с протоколами соревнований и расчетом ранга.

