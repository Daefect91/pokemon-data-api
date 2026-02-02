# candidate_solution.py
import os
import random
import sqlite3
from typing import List, Optional

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from httpx import Response
from starlette import status

# --- Constants ---
DB_NAME = "pokemon_assessment.db"


# --- Database Connection ---
def connect_db() -> Optional[sqlite3.Connection]:
    """
    Task 1: Connect to the SQLite database.
    Implement the connection logic and return the connection object.
    Return None if connection fails.
    """
    if not os.path.exists(DB_NAME):
        print(f"Error: Database file '{DB_NAME}' not found.")
        return None

    try:
        # --- Implement Here ---
        connection = sqlite3.connect(DB_NAME)
        # --- End Implementation ---
    except sqlite3.Error as e:
        print(f"Database connection error: {e}")
        return None

    return connection


# --- Data Cleaning ---
def clean_database(conn: sqlite3.Connection):
    """
    Task 2: Clean up the database using the provided connection object.
    Implement logic to:
    - Remove duplicate entries in tables (pokemon, types, abilities, trainers).
      Choose a consistent strategy (e.g., keep the first encountered/lowest ID).
    - Correct known misspellings (e.g., 'Pikuchu' -> 'Pikachu', 'gras' -> 'Grass', etc.).
    - Standardize casing (e.g., 'fire' -> 'Fire' or all lowercase for names/types/abilities).
    """
    if not conn:
        print("Error: Invalid database connection provided for cleaning.")
        return

    cursor = conn.cursor()
    print("Starting database cleaning...")

    try:
        # --- Implement Here ---
        tables = ['pokemon', 'types', 'abilities', 'trainers']
        for table in tables:
            # firstly, get the full set of names for the relevant table in the current iteration
            select_all_query = f"""
                    SELECT id, name FROM "{table}" 
                """
            names = cursor.execute(select_all_query).fetchall()

            # fix known spelling errors
            cursor = fix_known_spelling_errors(cursor, table, names)

            # fix casing by ensuring only the first letter of each word is uppercase
            names = cursor.execute(select_all_query).fetchall()
            cursor = fix_casing_for_all_names(cursor, table, names)

            # now that we have fixed the spelling and casing on all names, we can accurately remove duplicates
            cursor = remove_duplicates_and_align_foreign_keys(cursor, table)

            # lastly - remove placeholder, test and trash records
            cursor = remove_trash_from_table(cursor, table)
        # --- End Implementation ---
        conn.commit()
        print("Database cleaning finished and changes committed.")

    except sqlite3.Error as e:
        print(f"An error occurred during database cleaning: {e}")
        conn.rollback()  # Roll back changes on error


# --- Fix all known spelling errors in the supplied table using the list of names provided --- #
def fix_known_spelling_errors(cursor: sqlite3.Cursor, table: str, names: List[str]) -> sqlite3.Cursor:
    known_misspellings = {
        "charmanderr": "charmander",
        "gras": "grass",
        "poision": "poison",
        "bulbasuar": "bulbasaur",
        "pikuchu": "pikachu",
        "torrent": "torment"
    }

    spelling_updates_required = []
    for name in names:
        row_pk, original_name = name
        if isinstance(original_name, str):
            # Only update if the word is misspelt
            if known_misspellings.__contains__(original_name.lower()):
                spelling_updates_required.append(
                    (known_misspellings[original_name.lower()], row_pk))

    if spelling_updates_required:
        cursor.executemany(f"""UPDATE "{table}" SET name = ? WHERE id = ?""", spelling_updates_required)

    return cursor


# --- Fix all casing by applying the Title Case pattern to all names in the supplied table --- #
def fix_casing_for_all_names(cursor: sqlite3.Cursor, table: str, names: List[str]) -> sqlite3.Cursor:
    casing_updates_required = []
    for name in names:
        row_pk, original_name = name
        if isinstance(original_name, str):
            title_cased_name = original_name.title()
            # Only update if the name was actually changed
            if title_cased_name != original_name:
                casing_updates_required.append((title_cased_name, row_pk))

    if casing_updates_required:
        cursor.executemany(f"""UPDATE {table} SET name = ? WHERE id = ?""", casing_updates_required)

    return cursor


# --- Remove any duplicates by name from the supplied table and align foreign key relationships --- #
def remove_duplicates_and_align_foreign_keys(cursor: sqlite3.Cursor, table: str) -> sqlite3.Cursor:
    cursor = align_foreign_keys_based_on_table(cursor, table)

    cursor.execute(f"""delete from {table} where id not in (
                               select min(id) 
                                from {table} 
                                group by name
                           )""")

    return cursor


# --- Aligns the foreign keys based on the table currently being iterated for cleaning --- #
def align_foreign_keys_based_on_table(cursor: sqlite3.Cursor, table: str) -> sqlite3.Cursor:
    # first, get the list of duplicates that will be removed
    duplicates = cursor.execute(
        f"""select id, name from {table} where id not in (select min(id) from {table} group by name)""").fetchall()

    # align foreign keys to the id of the record that will be kept(lowest id)
    for duplicate in duplicates:
        min_id = cursor.execute(f"""select min(id) from {table} where lower(name) = :duplicate_name""",
                                {"duplicate_name": duplicate[1].lower()}).fetchone()
        if table == "types":
            cursor.execute(f"""update pokemon set type1_id = ? where type1_id = ?""", [min_id[0], duplicate[0]])
            cursor.execute(f"""update pokemon set type2_id = ? where type2_id = ?""", [min_id[0], duplicate[0]])
        else:
            foreign_key_column = None
            if table == "pokemon":
                foreign_key_column = "pokemon_id"
            elif table == "trainers":
                foreign_key_column = "trainer_id"
            elif table == "abilities":
                foreign_key_column = "ability_id"

            if foreign_key_column:
                cursor.execute(
                    f"""update trainer_pokemon_abilities set {foreign_key_column} = ? where {foreign_key_column} = ?""",
                    [min_id[0], duplicate[0]])

    return cursor


# --- Remove any trash or placeholder records from the supplied table --- #
def remove_trash_from_table(cursor: sqlite3.Cursor, table: str) -> sqlite3.Cursor:
    remove_trash_query = f"""delete
                            from {table}
                            where name in(?, ?, ?, ?)
                            or lower(name) like ?
                            """
    cursor.execute(remove_trash_query, ['', ' ', '???', '---', 'remove%'])
    return cursor


# --- FastAPI Application ---
def create_fastapi_app() -> FastAPI:
    """
    FastAPI application instance.
    Define the FastAPI app and include all the required endpoints below.
    """
    print("Creating FastAPI app and defining endpoints...")
    app = FastAPI(title="Pokemon Assessment API")

    # --- Define Endpoints Here ---
    @app.get("/")
    def read_root():
        """
        Task 3: Basic root response message
        Return a simple JSON response object that contains a `message` key with any corresponding value.
        """
        # --- Implement here ---
        return {"message": "Welcome to the Pokedex!"}
        # --- End Implementation ---

    @app.get("/pokemon/ability/{ability_name}", response_model=List[str])
    def get_pokemon_by_ability(ability_name: str):
        """
        Task 4: Retrieve all Pokémon names with a specific ability.
        Query the cleaned database. Handle cases where the ability doesn't exist.
        """
        # --- Implement here ---
        try:
            result = []
            db_conn = connect_db()
            cursor = db_conn.cursor()

            # check if the ability exists first
            ability_lookup = cursor.execute("select a.id from abilities a where lower(a.name) = :ability_name",
                                            {"ability_name": ability_name.lower()}).fetchone()
            if ability_lookup:
                get_by_ability_query = """select distinct p.name
                                          from pokemon p
                                                   join trainer_pokemon_abilities tpa on tpa.pokemon_id = p.id
                                          where tpa.ability_id = :ability_id
                                          order by p.name
                                       """
                pokemon = cursor.execute(get_by_ability_query, {"ability_id": ability_lookup[0]}).fetchall()
                if pokemon:
                    for p in pokemon:
                        result.append(p[0])
                else:
                    result.append(f"No pokemon found with ability '{ability_name}'")
            else:
                result.append(f"Ability '{ability_name}' does not exist")

            db_conn.close()
            return result
        except sqlite3.Error as e:
            print(f"Failed to connect to DB to retrieve Pokemon for supplied ability: {e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=f"""Could not retrieve the list of Pokemon with ability '{ability_name}', please try again later.""")
        # --- End Implementation ---

    @app.get("/pokemon/type/{type_name}", response_model=List[str])
    def get_pokemon_by_type(type_name: str):
        """
        Task 5: Retrieve all Pokémon names of a specific type (considers type1 and type2).
        Query the cleaned database. Handle cases where the type doesn't exist.
        """
        # --- Implement here ---
        try:
            result = []
            db_conn = connect_db()
            cursor = db_conn.cursor()

            # check if the type exists first
            type_lookup = cursor.execute("select t.id from types t where lower(t.name) = :type_name",
                                         {"type_name": type_name.lower()}).fetchone()
            if type_lookup:
                get_by_type_query = """select distinct p.name
                                       from pokemon p
                                       where p.type1_id = :type_id
                                          or p.type2_id = :type_id
                                       order by p.name
                                    """
                pokemon = cursor.execute(get_by_type_query, {"type_id": type_lookup[0]}).fetchall()

                if pokemon:
                    for p in pokemon:
                        result.append(p[0])
                else:
                    result.append(f"No pokemon found of type '{type_name}'")
            else:
                result.append(f"Type '{type_name}' does not exist")

            db_conn.close()
            return result
        except sqlite3.Error as e:
            print(f"Failed to connect to DB to retrieve Pokemon for supplied type: {e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=f"""Could not retrieve the list of Pokemon of type '{type_name}', please try again later.""")
        # --- End Implementation ---

    @app.get("/trainers/pokemon/{pokemon_name}", response_model=List[str])
    def get_trainers_by_pokemon(pokemon_name: str):
        """
        Task 6: Retrieve all trainer names who have a specific Pokémon.
        Query the cleaned database. Handle cases where the Pokémon doesn't exist or has no trainer.
        """
        # --- Implement here ---
        try:
            result = []
            db_conn = connect_db()
            cursor = db_conn.cursor()

            # check if the pokemon exists first
            pokemon_lookup = cursor.execute("select p.id from pokemon p where lower(p.name) = :pokemon_name",
                                            {"pokemon_name": pokemon_name.lower()}).fetchone()
            if pokemon_lookup:
                get_trainers_by_pokemon_query = """select distinct t.name
                                                   from trainers t
                                                            join trainer_pokemon_abilities tpa on tpa.trainer_id = t.id
                                                   where tpa.pokemon_id = :pokemon_id
                                                   order by t.name
                                                """
                trainers = cursor.execute(get_trainers_by_pokemon_query, {"pokemon_id": pokemon_lookup[0]}).fetchall()

                if trainers:
                    for trainer in trainers:
                        result.append(trainer[0])
                else:
                    result.append(f"No trainers found who use '{pokemon_name}'")
            else:
                result.append(f"Pokemon with name '{pokemon_name}' does not exist")

            db_conn.close()
            return result
        except sqlite3.Error as e:
            print(f"Failed to connect to DB to retrieve Trainers for supplied Pokemon: {e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=f"""Could not retrieve the list of Trainers who use '{pokemon_name}', please try again later.""")
        # --- End Implementation ---

    @app.get("/abilities/pokemon/{pokemon_name}", response_model=List[str])
    def get_abilities_by_pokemon(pokemon_name: str):
        """
        Task 7: Retrieve all ability names of a specific Pokémon.
        Query the cleaned database. Handle cases where the Pokémon doesn't exist.
        """
        # --- Implement here ---
        try:
            result = []
            db_conn = connect_db()
            cursor = db_conn.cursor()

            # check if the pokemon exists first
            pokemon_lookup = cursor.execute("select p.id from pokemon p where lower(p.name) = :pokemon_name",
                                            {"pokemon_name": pokemon_name.lower()}).fetchone()
            if pokemon_lookup:
                get_abilities_by_pokemon_query = """select distinct a.name
                                                    from abilities a
                                                             join trainer_pokemon_abilities tpa on tpa.ability_id = a.id
                                                    where tpa.pokemon_id = :pokemon_id
                                                    order by a.name
                                                 """
                abilities = cursor.execute(get_abilities_by_pokemon_query, {"pokemon_id": pokemon_lookup[0]}).fetchall()

                if abilities:
                    for ability in abilities:
                        result.append(ability[0])
                else:
                    result.append(f"No abilities found for Pokemon '{pokemon_name}'")
            else:
                result.append(f"Pokemon with name '{pokemon_name}' does not exist")

            db_conn.close()
            return result
        except sqlite3.Error as e:
            print(f"Failed to connect to DB to retrieve Abilities for supplied Pokemon: {e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=f"""Could not retrieve the list of Abilities for '{pokemon_name}', please try again later.""")
        # --- End Implementation ---

    # --- Implement Task 8 here ---
    @app.post("/pokemon/create/{pokemon_name}", response_model=int)
    async def create_pokemon(pokemon_name: str):
        db_conn = connect_db()
        cursor = db_conn.cursor()

        # first check if the pokemon already exists
        pokemon_lookup = cursor.execute(f"""select p.id from pokemon p where lower(p.name) = :pokemon_name""",
                                        {"pokemon_name": pokemon_name.lower()}).fetchone()
        if pokemon_lookup:
            return pokemon_lookup[0]
        else:
            # get the pokemon's relevant data from external api
            response = await retrieve_pokemon_data_from_external_api(pokemon_name)

            # process the response from external api by extracting the relevant abilities, types etc.
            if response:
                try:
                    pokemon_data = response.json()
                    abilities = pokemon_data["abilities"]
                    types = pokemon_data["types"]

                    # handle the pokemon's types first
                    new_pokemon, cursor = process_pokemon_types(cursor, pokemon_name, types)

                    # create a new pokemon lookup
                    cursor.execute(
                        f"""insert into pokemon (name, type1_id, type2_id) values (:name, :type1_id, :type2_id)""",
                        new_pokemon)
                    new_pokemon_lookup = cursor.execute(f"""select p.id from pokemon p where _ROWID_ = :last_rowid""",
                                                        {"last_rowid": cursor.lastrowid}).fetchone()
                    pokemon_id = new_pokemon_lookup[0]

                    # once we have the types and pokemon lookups created, we can process the abilities
                    process_pokemon_abilities(cursor, pokemon_id, abilities)

                    db_conn.commit()
                    db_conn.close()
                    return pokemon_id
                except Exception as e:
                    print(f"Failed to create pokemon {pokemon_name} - {e}")
                    db_conn.close()
                    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                        detail=f"""Error creating pokemon '{pokemon_name}', please try again later.""")
            else:
                db_conn.close()
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error creating pokemon, please ensure you are using a valid pokemon name")

    # --- End Implementation ---

    print("FastAPI app created successfully.")
    return app


# --- Retrieve the pokemon's data from the external API --- #
async def retrieve_pokemon_data_from_external_api(pokemon_name: str) -> Optional[Response]:
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_name}"
    response = None
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
        except httpx.HTTPStatusError:
            # Handle specific HTTP errors
            raise HTTPException(status_code=response.status_code,
                                detail="Error fetching data from external Pokemon API")
        except httpx.RequestError:
            # Handle network-related errors (DNS issues, connection refused, etc.)
            raise HTTPException(status_code=500, detail="Network error when connecting to external Pokemon API")
    return response


# --- Process the pokemon's types, adding any lookups that don't exist yet --- #
def process_pokemon_types(cursor: sqlite3.Cursor, pokemon_name: str, types: list) -> tuple[dict, sqlite3.Cursor]:
    new_pokemon = {"name": pokemon_name.title(), "type1_id": None, "type2_id": None}
    for pokemon_type in types:
        type_name = pokemon_type["type"]["name"]
        type_lookup = cursor.execute(f"""select t.id from types t where lower(t.name) = :type_name""",
                                     {"type_name": type_name.lower()}).fetchone()
        if not type_lookup:
            # create new type lookup
            cursor.execute("""insert into types (name)
                              values (:type_name)""", {"type_name": type_name.title()})
            new_type_lookup = cursor.execute(f"""select t.id from types t where _ROWID_ = :last_rowid""",
                                             {"last_rowid": cursor.lastrowid}).fetchone()
            type_id = new_type_lookup[0]
        else:
            type_id = type_lookup[0]

        if not new_pokemon.get("type1_id"):
            new_pokemon.__setitem__("type1_id", type_id)
        elif not new_pokemon.get("type2_id"):
            new_pokemon.__setitem__("type2_id", type_id)
    return new_pokemon, cursor


# --- Process the pokemon's abilities and link them to a random trainer id --- #
def process_pokemon_abilities(cursor: sqlite3.Cursor, pokemon_id: int, abilities: list):
    trainer_ids = cursor.execute(f"""select distinct t.id from trainers t""").fetchall()
    for ability in abilities:
        ability_name = ability["ability"]["name"]
        ability_name = ability_name.replace('-', ' ')

        ability_lookup = cursor.execute(f"""select a.id from abilities a where lower(a.name) = :ability_name""",
                                        {"ability_name": ability_name.lower()}).fetchone()
        if not ability_lookup:
            # create new ability lookup
            cursor.execute(f"""insert into abilities (name) values (:ability_name)""",
                           {"ability_name": ability_name.title()})
            new_ability_lookup = cursor.execute(f"""select a.id from abilities a where _ROWID_ = :last_rowid""",
                                                {"last_rowid": cursor.lastrowid}).fetchone()
            ability_id = new_ability_lookup[0]
        else:
            ability_id = ability_lookup[0]

        # create trainer pokemon ability record
        trainer_id = random.choice(trainer_ids)[0]
        trainer_pokemon_ability = {"pokemon_id": pokemon_id, "trainer_id": trainer_id, "ability_id": ability_id}
        cursor.execute(
            f"""insert into trainer_pokemon_abilities (pokemon_id, trainer_id, ability_id) values (:pokemon_id, :trainer_id, :ability_id)""",
            trainer_pokemon_ability)


# --- Main execution / Uvicorn setup (Optional - for candidate to run locally) ---
if __name__ == "__main__":
    # Ensure data is cleaned before running the app for testing
    temp_conn = connect_db()
    if temp_conn:
        clean_database(temp_conn)
        temp_conn.close()

    app_instance = create_fastapi_app()
    uvicorn.run(app_instance, host="127.0.0.1", port=8000)
