# events-schemas

Схемы для событийных данных

### Форматирование

Необходимо соблюдать:
- отступы в схеме - 2 пробела
- пробел после ":"

Правильно:
```json
{
  "bannerTypes": {
    "type": "array",
    "description": "Структура экрана",
    "items": {
      "description": "Тип баннера",
      "type": "string"
    }
  }
}
```

Неправильно:
```json
{
   "bannerTypes": {
    "type":"array",
    "description":"Структура экрана",
    "items": {
        "description":"Тип баннера",
        "type":"string"
      }
   }
}
```

Если отступы не будут соблюдены, пайп упадет с ошибкой:
"Error: File shop_selection_started.json is formatted incorrectly, please use ⇧⌥F in WebIDE, or JSON formatter e.g. https://jsonformatter.org/. Tab must be 2 spaces"

### Основные положения
1) Схемы хранятся в папке `new_app/schema/events`
2) В папке `new_app/schema/defintions` лежат переиспользуемые сущности
3) Схема должна содержать как минимум следующие поля:
   1) `description` - Описание события/параметра
   2) `properties` - Объект с параметрами, может быть пустым `{}`
   3) `title` - Наименование события, например `Some Button Clicked`
   4) `type` - Тип параметра. Для корневого объекта всегда `object`
Пример минимально валидной схемы события:
```json
{
  "description": "Нажата кнопка 'Подробнее' на экране профиля",
  "properties": {},
  "title": "Profile Show More Button Clicked",
  "type": "object"
}
```
4) Схема может содержать помимо обязательных следующие поля:
   1) `$comment` - Строковый комментарий
   2) `required` - Массив, содержащий обязательные параметры, например, если параметр `isActive` является обязательным:
```json
{
  "description": "Нажата кнопка 'Подробнее' на экране профиля",
  "properties": {
    "isActive": {
      "type": "boolean",
      "description": "Была ли кнопка активна в момент нажатия"
    }
  },
  "title": "Profile Show More Button Clicked",
  "type": "object",
  "$comment": "Я - коментарий",
  "required": ["isActive"]
}
```
5) Параметры могут быть следующих типов:
   1) `string` - строка
   2) `number` - число
   3) `boolean` - true | false
   4) `object` - объект
   5) `array` - массив


### Использование `definitions`
Если какая-либо сущность очень громоздкая или переиспользуется в более чем 2ух местах,
стоит вынести ее описание в папку `new_app/schema/defintions`.

Для этого в папке нужно создать новый файл, придерживаясь правила нейминга `PascalCase.json`
(Слитно, каждое слово с большой буквы) и записать в этот файл вашу сущность, например
```json
{
  "title": "CategorySuggestion",
  "type": "object",
  "description": "Саджест категории",
  "properties": {
    "identifier": {
      "description": "Идентификатор саджеста",
      "type": "string"
    },
    "position": {
      "description": "Порядковый номер в списке",
      "type": "number"
    }
  },
  "required": [
    "identifier",
    "position"
  ]
}
```
Правила описания совпадают с правилами описания любого параметра или события

После этого `definiton` можно применять в описании вашей схемы, например
```json
{
  "description": "Нажата кнопка 'Подробнее' на экране профиля",
  "properties": {
    "suggestions": {
      "description": "Предложенные варианты",
      "$ref": "#/definitions/CategorySuggestion"
    }
  },
  "title": "Profile Show More Button Clicked",
  "type": "object",
  "required": ["isActive"]
}
```

### Другие примеры описания полей:

#### enum - строковый параметр с ограниченым списком значений:
```json
{
  "bannerType": {
    "description": "Тип баннера",
    "type": "string",
    "enum": [
      "link",
      "promo"
    ]
  }
}
```

#### string - строковый параметр
```json
{
  "bannerType": {
    "description": "Тип баннера",
    "type": "string"
  }
}
```

#### boolean - бинарный параметр
```json
{
  "isActive": {
    "type": "boolean",
    "description": "Была ли кнопка активна в момент нажатия"
  }
}
```

#### reference - параметр ссылающийся на `definitions`
```json
{
  "suggestions": {
    "description": "Предложенные варианты",
    "$ref": "#/definitions/CategorySuggestion"
  }
}
```

#### object - объектный параметр состоит из вложенных параметров
Поле `properties` отвечает за описание вложенных параметров
```json
{
  "structure": {
    "type": "object",
    "description": "Структура экрана",
    "properties": {
      "isActive": {
        "type": "boolean",
        "description": "Была ли кнопка активна в момент нажатия"
      },
      "bannerType": {
        "description": "Тип баннера",
        "type": "string",
        "enum": [
          "link",
          "promo"
        ]
      }
    }
  }
}
```
**Примечание**: поле `title` является обязательным для корневого объекта (события), но не является обязательным для вложенных

#### array - список значений одного типа.
Поле `items` отвечает за описание типа элементов списка, может быть любым из доступных типов
```json
{
  "bannerTypes": {
    "type": "array",
    "description": "Структура экрана",
    "items": {
      "description": "Тип баннера",
      "type": "string"
    }
  }
}
```
