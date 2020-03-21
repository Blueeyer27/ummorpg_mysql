
using UnityEngine;
using Mirror;
using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using MySql.Data;                               // From MySql.Data.dll in Plugins folder
using MySql.Data.MySqlClient;                   // From MySql.Data.dll in Plugins folder


using SqlParameter = MySql.Data.MySqlClient.MySqlParameter;
using UnityEngine.AI;


/// <summary>
/// Database class for mysql
/// Port of the sqlite database class from ummorpg
/// </summary>
public partial class Database : MonoBehaviour
{
    public static Database singleton;

    private string connectionString = null;
    /// <summary>
    /// produces the connection string based on environment variables
    /// </summary>
    /// <value>The connection string</value>
    private string ConnectionString
    {
        get
        {

            if (connectionString == null)
            {
                var connectionStringBuilder = new MySqlConnectionStringBuilder
                {
                    Server = GetEnv("MYSQL_HOST") ?? "localhost",
                    Database = GetEnv("MYSQL_DATABASE") ?? "mmo_db",
                    UserID = GetEnv("MYSQL_USER") ?? "ummorpg",
                    Password = GetEnv("MYSQL_PASSWORD") ?? "p@ssword",
                    Port = GetUIntEnv("MYSQL_PORT", 3306),
                    CharacterSet = "utf8",
                    OldGuids=true
                };
                connectionString = connectionStringBuilder.ConnectionString;
            }

            return connectionString;
        }
    }

    private void Transaction(Action<MySqlCommand> action)
    {
        using (var connection = new MySqlConnection(ConnectionString))
        {

            connection.Open();
            MySqlTransaction transaction = null;

            try
            {

                transaction = connection.BeginTransaction();

                MySqlCommand command = new MySqlCommand();
                command.Connection = connection;
                command.Transaction = transaction;

                action(command);

                transaction.Commit();

            }
            catch (Exception ex)
            {
                if (transaction != null)
                    transaction.Rollback();
                throw ex;
            }
        }
    }

    private static string GetEnv(string name)
    {
        return Environment.GetEnvironmentVariable(name);

    }

    private static uint GetUIntEnv(string name, uint defaultValue = 0)
    {
        var value = Environment.GetEnvironmentVariable(name);

        if (value == null)
            return defaultValue;

        uint result;

        if (uint.TryParse(value, out result))
            return result;

        return defaultValue;
    }

    private void InitializeSchema()
    {
        ExecuteNonQueryMySql(@"
        CREATE TABLE IF NOT EXISTS accounts(
            name VARCHAR(16) NOT NULL,
            password CHAR(50) NOT NULL,
            created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            lastlogin DATETIME,
            banned BOOLEAN NOT NULL DEFAULT 0,
            PRIMARY KEY(name)
        ) CHARACTER SET=utf8mb4");

        ExecuteNonQueryMySql(@"
        CREATE TABLE IF NOT EXISTS characters(
            name VARCHAR(16) NOT NULL,
            account VARCHAR(16) NOT NULL,
            classname VARCHAR(16) NOT NULL,
            x FLOAT NOT NULL,
            y FLOAT NOT NULL,
            z FLOAT NOT NULL,
            level INT NOT NULL DEFAULT 1,
            health INT NOT NULL,
            mana INT NOT NULL,
            strength INT NOT NULL DEFAULT 0,
            intelligence INT NOT NULL DEFAULT 0,
            experience BIGINT NOT NULL DEFAULT 0,
            skillExperience BIGINT NOT NULL DEFAULT 0,
            gold BIGINT NOT NULL DEFAULT 0,
            coins BIGINT NOT NULL DEFAULT 0,

            online BOOLEAN NOT NULL DEFAULT 0,
            lastsaved DATETIME,
            deleted BOOLEAN NOT NULL DEFAULT 0,

            PRIMARY KEY (name),
            INDEX(account),
            FOREIGN KEY(account)
                REFERENCES accounts(name)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) CHARACTER SET=utf8mb4");

        ExecuteNonQueryMySql(@"
        CREATE TABLE IF NOT EXISTS character_inventory(
            `character` VARCHAR(16) NOT NULL,
            slot INT NOT NULL,
            name VARCHAR(50) NOT NULL,
            amount INT NOT NULL,
            summonedHealth INT NOT NULL,
            summonedLevel INT NOT NULL,
            summonedExperience BIGINT NOT NULL,

            PRIMARY KEY(`character`, slot),
            FOREIGN KEY(`character`)
                REFERENCES characters(name)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) CHARACTER SET=utf8mb4");

        ExecuteNonQueryMySql(@"
        CREATE TABLE IF NOT EXISTS character_equipment(
            `character` VARCHAR(16) NOT NULL,
            slot INT NOT NULL,
            name VARCHAR(50) NOT NULL,
            amount INT NOT NULL,
            summonedHealth INT NOT NULL,
            summonedLevel INT NOT NULL,
            summonedExperience BIGINT NOT NULL,

            PRIMARY KEY(`character`, slot),
            FOREIGN KEY(`character`)
                REFERENCES characters(name)
                ON DELETE CASCADE ON UPDATE CASCADE
         ) CHARACTER SET=utf8mb4");

        ExecuteNonQueryMySql(@"
        CREATE TABLE IF NOT EXISTS character_skills(
            `character` VARCHAR(16) NOT NULL,
            name VARCHAR(50) NOT NULL,
            level INT NOT NULL,
            castTimeEnd FLOAT NOT NULL,
            cooldownEnd FLOAT NOT NULL,

            PRIMARY KEY (`character`, name),
            FOREIGN KEY(`character`)
                REFERENCES characters(name)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) CHARACTER SET=utf8mb4");

        ExecuteNonQueryMySql(@"
        CREATE TABLE IF NOT EXISTS character_buffs(
            `character` VARCHAR(16) NOT NULL,
            name VARCHAR(50) NOT NULL,
            level INT NOT NULL,
            buffTimeEnd FLOAT NOT NULL,

            PRIMARY KEY (`character`, name),
            FOREIGN KEY(`character`)
                REFERENCES characters(name)
                ON DELETE CASCADE ON UPDATE CASCADE 
        ) CHARACTER SET=utf8mb4");

        ExecuteNonQueryMySql(@"
        CREATE TABLE IF NOT EXISTS character_quests(
            `character` VARCHAR(16) NOT NULL,
            name VARCHAR(50) NOT NULL,
            progress INT NOT NULL,
            completed BOOLEAN NOT NULL,

            PRIMARY KEY(`character`, name),
            FOREIGN KEY(`character`)
                REFERENCES characters(name)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) CHARACTER SET=utf8mb4");

        ExecuteNonQueryMySql(@"
        CREATE TABLE IF NOT EXISTS character_orders(
            orderid BIGINT NOT NULL AUTO_INCREMENT,
            `character` VARCHAR(16) NOT NULL,
            coins BIGINT NOT NULL,
            processed BIGINT NOT NULL,

            PRIMARY KEY(orderid),
            INDEX(`character`),
            FOREIGN KEY(`character`)
                REFERENCES characters(name)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) CHARACTER SET=utf8mb4");

        ExecuteNonQueryMySql(@"
        CREATE TABLE IF NOT EXISTS guild_info(
            name VARCHAR(16) NOT NULL,
            notice TEXT NOT NULL,
            PRIMARY KEY(name)
        ) CHARACTER SET=utf8mb4");

        ExecuteNonQueryMySql(@"
        CREATE TABLE IF NOT EXISTS character_guild(
            `character` VARCHAR(16) UNIQUE NOT NULL,
            guild VARCHAR(16) NOT NULL,
            `rank` INT NOT NULL DEFAULT 0,
            FOREIGN KEY(`character`)
                REFERENCES characters(name)
                ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY(guild)
                REFERENCES guild_info(name)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) CHARACTER SET=utf8mb4");
    }

    void Awake()
    {
        // initialize singleton
        if (singleton == null) singleton = this;
    }

    public void Connect()
    {
        InitializeSchema();

        // addon system hooks
        Utils.InvokeMany(typeof(Database), this, "Initialize_"); // TODO remove later. let's keep the old hook for a while to not break every single addon!
        Utils.InvokeMany(typeof(Database), this, "Connect_"); // the new hook!
    }

    #region Helper Functions
    // run a query that doesn't return anything
    private void ExecuteNonQueryMySql(string sql, params SqlParameter[] args)
    {
        try
        {
            MySqlHelper.ExecuteNonQuery(ConnectionString, sql, args);
        }
        catch (Exception ex)
        {
            Debug.LogErrorFormat("Failed to execute query {0}", sql);
            throw ex;
        }

    }


    private void ExecuteNonQueryMySql(MySqlCommand command, string sql, params SqlParameter[] args)
    {
        try
        {
            command.CommandText = sql;
            command.Parameters.Clear();

            foreach (var arg in args)
            {
                command.Parameters.Add(arg);
            }

            command.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            Debug.LogErrorFormat("Failed to execute query {0}", sql);
            throw ex;
        }

    }

    // run a query that returns a single value
    private object ExecuteScalarMySql(string sql, params SqlParameter[] args)
    {
        try
        {
            return MySqlHelper.ExecuteScalar(ConnectionString, sql, args);
        }
        catch (Exception ex)
        {
            Debug.LogErrorFormat("Failed to execute query {0}", sql);
            throw ex;
        }
    }

    private DataRow ExecuteDataRowMySql(string sql, params SqlParameter[] args)
    {
        try
        {
            return MySqlHelper.ExecuteDataRow(ConnectionString, sql, args);
        }
        catch (Exception ex)
        {
            Debug.LogErrorFormat("Failed to execute query {0}", sql);
            throw ex;
        }
    }

    private DataSet ExecuteDataSetMySql(string sql, params SqlParameter[] args)
    {
        try
        {
            return MySqlHelper.ExecuteDataset(ConnectionString, sql, args);
        }
        catch (Exception ex)
        {
            Debug.LogErrorFormat("Failed to execute query {0}", sql);
            throw ex;
        }
    }

    // run a query that returns several values
    private List<List<object>> ExecuteReaderMySql(string sql, params SqlParameter[] args)
    {
        try
        {
            var result = new List<List<object>>();

            using (var reader = MySqlHelper.ExecuteReader(ConnectionString, sql, args))
            {

                while (reader.Read())
                {
                    var buf = new object[reader.FieldCount];
                    reader.GetValues(buf);
                    result.Add(buf.ToList());
                }
            }

            return result;
        }
        catch (Exception ex)
        {
            Debug.LogErrorFormat("Failed to execute query {0}", sql);
            throw ex;
        }

    }

    // run a query that returns several values
    private MySqlDataReader GetReader(string sql, params SqlParameter[] args)
    {
        try
        {
            return MySqlHelper.ExecuteReader(ConnectionString, sql, args);
        }
        catch (Exception ex)
        {
            Debug.LogErrorFormat("Failed to execute query {0}", sql);
            throw ex;
        }
    }
    #endregion


    // account data ////////////////////////////////////////////////////////////
    public bool TryLogin(string account, string password)
    {
        if (!string.IsNullOrWhiteSpace(account) && !string.IsNullOrWhiteSpace(password))
        {

            var row = ExecuteDataRowMySql("SELECT password, banned FROM accounts WHERE name=@name", new SqlParameter("@name", account));
            if (row != null)
            {
                return !(bool)row["banned"] && password == (string)row["password"];
            }

            // Account doesn't exist. Let's try to create it.
            var currentTime = DateTime.UtcNow;
            ExecuteNonQueryMySql("INSERT INTO accounts VALUES (@name, @password, @created, @lastlogin, 0)",
                new SqlParameter("@name", account), new SqlParameter("@password", password),
                new SqlParameter("@created", currentTime), new SqlParameter("@lastlogin", currentTime));

            return true;
        }

        return false;
    }

    // character data //////////////////////////////////////////////////////////
    public bool CharacterExists(string characterName)
    {
        // checks deleted ones too so we don't end up with duplicates if we un-
        // delete one
        return ((long)ExecuteScalarMySql("SELECT Count(*) FROM characters WHERE name=@name", new SqlParameter("@name", characterName))) > 0;
    }

    public void CharacterDelete(string characterName)
    {
        // soft delete the character so it can always be restored later
        ExecuteNonQueryMySql("UPDATE characters SET deleted=1 WHERE name=@character", new SqlParameter("@character", characterName));
    }

    // returns a dict of<character name, character class=prefab name>
    // we really need the prefab name too, so that client character selection
    // can read all kinds of properties like icons, stats, 3D models and not
    // just the character name
    public List<string> CharactersForAccount(string account)
    {
        var result = new List<string>();

        var table = ExecuteReaderMySql("SELECT name FROM characters WHERE account=@account AND deleted=0", new SqlParameter("@account", account));
        foreach (var row in table)
            result.Add((string)row[0]);

        return result;
    }

    private void LoadInventory(Player player)
    {
        // fill all slots first
        for (int i = 0; i < player.inventorySize; ++i)
            player.inventory.Add(new ItemSlot());

        // override with the inventory stored in database
        using (var reader = GetReader("SELECT * FROM character_inventory WHERE `character`=@character", new SqlParameter("@character", player.name)))
        {
            while (reader.Read())
            {
                string itemName = (string)reader["name"];
                int slot = (int)reader["slot"];

                if (slot < player.inventorySize)
                {
                    if (ScriptableItem.dict.TryGetValue(itemName.GetStableHashCode(), out ScriptableItem itemData))
                    {
                        Item item = new Item(itemData);
                        item.summonedHealth = (int)reader["summonedHealth"];
                        item.summonedLevel = (int)reader["summonedLevel"];
                        item.summonedExperience = (long)reader["summonedExperience"];

                        int amount = (int)reader["amount"];
                        player.inventory[slot] = new ItemSlot(item, amount);
                    }
                    else
                    {
                        Debug.LogWarning("LoadInventory: skipped item " + itemName + " for " + player.name + " because it doesn't exist anymore. If it wasn't removed intentionally then make sure it's in the Resources folder.");
                    }
                }
                else
                {
                    Debug.LogWarning("LoadInventory: skipped slot " + slot + " for " + player.name + " because it's bigger than size " + player.inventorySize);
                }
            }
        }
    }

    private void LoadEquipment(Player player)
    {
        // fill all slots first
        for (int i = 0; i < player.equipmentInfo.Length; ++i)
            player.equipment.Add(new ItemSlot());
       
        using (var reader = GetReader("SELECT * FROM character_equipment WHERE `character`=@character", new SqlParameter("@character", player.name)))
        {
            while (reader.Read())
            {
                string itemName = (string)reader["name"];
                int slot = (int)reader["slot"];

                if (slot < player.equipmentInfo.Length)
                {
                    if (ScriptableItem.dict.TryGetValue(itemName.GetStableHashCode(), out ScriptableItem itemData))
                    {
                        Item item = new Item(itemData);
                        int amount = (int)reader["amount"];
                        player.equipment[slot] = new ItemSlot(item, amount);
                    }
                    else
                    {
                        Debug.LogWarning("LoadEquipment: skipped item " + itemName + " for " + player.name + " because it doesn't exist anymore. If it wasn't removed intentionally then make sure it's in the Resources folder.");
                    }
                }
                else
                {
                    Debug.LogWarning("LoadEquipment: skipped slot " + slot + " for " + player.name + " because it's bigger than size " + player.equipmentInfo.Length);
                }
        }
        }
    }

    private void LoadSkills(Player player)
    {
        // load skills based on skill templates (the others don't matter)
        // -> this way any template changes in a prefab will be applied
        //    to all existing players every time (unlike item templates
        //    which are only for newly created characters)

        // fill all slots first
        foreach (ScriptableSkill skillData in player.skillTemplates)
            player.skills.Add(new Skill(skillData));

        using (var reader = GetReader("SELECT * FROM character_skills WHERE `character`=@character", new SqlParameter("@character", player.name)))
        {
            while (reader.Read())
            {
                var skillName = (string)reader["name"];

                int index = player.skills.FindIndex(skill => skill.name == skillName);
                if (index != -1)
                {
                    Skill skill = player.skills[index];
                    // make sure that 1 <= level <= maxlevel (in case we removed a skill
                    // level etc)
                    skill.level = Mathf.Clamp((int)reader["level"], 1, skill.maxLevel);
                    // make sure that 1 <= level <= maxlevel (in case we removed a skill
                    // level etc)
                    // castTimeEnd and cooldownEnd are based on Time.time, which
                    // will be different when restarting a server, hence why we
                    // saved them as just the remaining times. so let's convert them
                    // back again.
                    skill.castTimeEnd = (float)reader["castTimeEnd"] + NetworkTime.time;
                    skill.cooldownEnd = (float)reader["cooldownEnd"] + NetworkTime.time;

                    player.skills[index] = skill;
                }
            }
        }
    }

    private void LoadBuffs(Player player)
    {

        using (var reader = GetReader("SELECT name, level, buffTimeEnd FROM character_buffs WHERE `character`=@character", new SqlParameter("@character", player.name)))
        {
            while (reader.Read())
            {
                string buffName = (string)reader["name"];
                if (ScriptableSkill.dict.TryGetValue(buffName.GetStableHashCode(), out ScriptableSkill skillData))
                {
                    // make sure that 1 <= level <= maxlevel (in case we removed a skill
                    // level etc)
                    int level = Mathf.Clamp((int)reader["level"], 1, skillData.maxLevel);
                    Buff buff = new Buff((BuffSkill)skillData, level);
                    // buffTimeEnd is based on Time.time, which will be
                    // different when restarting a server, hence why we saved
                    // them as just the remaining times. so let's convert them
                    // back again.
                    buff.buffTimeEnd = (float)reader["buffTimeEnd"] + Time.time;
                    player.buffs.Add(buff);
                }
                else
                {
                    Debug.LogWarning("LoadBuffs: skipped buff " + buffName + " for " + player.name + " because it doesn't exist anymore. If it wasn't removed intentionally then make sure it's in the Resources folder.");
                }
            }
        }
    }

    private void LoadQuests(Player player)
    {
        // load quests
        using (var reader = GetReader("SELECT * FROM character_quests WHERE `character`=@character", new SqlParameter("@character", player.name)))
        {
            while (reader.Read())
            {
                string questName = (string)reader["name"];

                if (ScriptableQuest.dict.TryGetValue(questName.GetStableHashCode(), out ScriptableQuest questData))
                {
                    Quest quest = new Quest(questData);
                    quest.progress = (int)reader["progress"];
                    quest.completed = (bool)reader["completed"];
                    player.quests.Add(quest);
                }
                else
                {
                    Debug.LogWarning("LoadQuests: skipped quest " + questName + " for " + player.name + " because it doesn't exist anymore. If it wasn't removed intentionally then make sure it's in the Resources folder.");
                }
            }
        }
    }

    // only load guild when their first player logs in
    // => using NetworkManager.Awake to load all guilds.Where would work,
    //    but we would require lots of memory and it might take a long time.
    // => hooking into player loading to load guilds is a really smart solution,
    //    because we don't ever have to load guilds that aren't needed
    private void LoadGuildOnDemand(Player player)
    {
        var row = ExecuteDataRowMySql("SELECT guild FROM character_guild WHERE `character`=@character", new SqlParameter("@character", player.name));
        if (row != null)
        {
            string guildName = (string)row["guild"];

            // load guild on demand when the first player of that guild logs in
            // (= if it's not in GuildSystem.guilds yet)
            if (!GuildSystem.guilds.ContainsKey(guildName))
            {
                Guild guild = LoadGuild(guildName);
                GuildSystem.guilds[guild.name] = guild;
                player.guild = guild;
            }
            // assign from already loaded guild
            else
            {
                player.guild = GuildSystem.guilds[guildName];
            }
        }
    }

    public GameObject CharacterLoad(string characterName, List<Player> prefabs, bool isPreview)
    {
        var row = ExecuteDataRowMySql("SELECT * FROM characters WHERE name=@name AND deleted=0", new SqlParameter("@name", characterName));
        if (row != null)
        {
            // instantiate based on the class name
            string className = (string)row["classname"];
            var prefab = prefabs.Find(p => p.name == className);
            if (prefab != null)
            {
                var go = Instantiate(prefab.gameObject);
                var player = go.GetComponent<Player>();

                player.name             = (string)row["name"];
                player.account          = (string)row["account"];
                player.className        = className;

                float x                 = (float)row["x"];
                float y                 = (float)row["y"];
                float z                 = (float)row["z"];
                Vector3 position        = new Vector3(x, y, z);

                player.level            = Mathf.Min((int)row["level"], player.maxLevel);

                int health              = (int)row["health"];
                int mana                = (int)row["mana"];

                player.strength         = (int)row["strength"];
                player.intelligence     = (int)row["intelligence"];
                player.experience       = (long)row["experience"];
                player.skillExperience  = (long)row["skillExperience"];
                player.gold             = (long)row["gold"];
                player.coins            = (long)row["coins"];

                // is the position on a navmesh?
                // it might not be if we changed the terrain, or if the player
                // logged out in an instanced dungeon that doesn't exist anymore
                if (NavMesh.SamplePosition(position, out NavMeshHit hit, 0.1f, NavMesh.AllAreas))
                {
                    // warp is recommended over transform.position and
                    // avoids all kinds of weird bugs
                    player.Warp(position);
                }
                // otherwise warp to start position
                else
                {
                    Transform start = NetworkManagerMMO.GetNearestStartPosition(position);
                    player.Warp(start.position);
                    // no need to show the message all the time. it would spam
                    // the server logs too much.
                    //Debug.Log(player.name + " spawn position reset because it's not on a NavMesh anymore. This can happen if the player previously logged out in an instance or if the Terrain was changed.");
                }

                LoadInventory(player);
                LoadEquipment(player);
                LoadSkills(player);
                LoadBuffs(player);
                LoadQuests(player);
                LoadGuildOnDemand(player);

                // assign health / mana after max values were fully loaded
                // (they depend on equipment, buffs, etc.)
                player.health = health;
                player.mana = mana;

                // set 'online' directly. otherwise it would only be set during
                // the next CharacterSave() call, which might take 5-10 minutes.
                // => don't set it when loading previews though. only when
                //    really joining the world (hence setOnline flag)
                if (!isPreview)
                    ExecuteNonQueryMySql("UPDATE characters SET online=1, lastsaved=@lastsaved WHERE name=@name",
                        new SqlParameter("@lastsaved", DateTime.UtcNow), new SqlParameter("@name", characterName));

                // addon system hooks
                Utils.InvokeMany(typeof(Database), this, "CharacterLoad_", player);

                return go;
            }

            Debug.LogError("no prefab found for class: " + className);
        }

        return null;
    }

    void SaveInventory(Player player, MySqlCommand command)
    {
        // inventory: remove old entries first, then add all new ones
        // (we could use UPDATE where slot=... but deleting everything makes
        //  sure that there are never any ghosts)
        ExecuteNonQueryMySql(command, "DELETE FROM character_inventory WHERE `character`=@character", new SqlParameter("@character", player.name));
        for (int i = 0; i < player.inventory.Count; ++i)
        {
            ItemSlot slot = player.inventory[i];
            if (slot.amount > 0) // only relevant items to save queries/storage/time
            {
                ExecuteNonQueryMySql(command, "INSERT INTO character_inventory VALUES (@character, @slot, @name, @amount, @summonedHealth, @summonedLevel, @summonedExperience)",
                        new SqlParameter("@character", player.name),
                        new SqlParameter("@slot", i),
                        new SqlParameter("@name", slot.item.name),
                        new SqlParameter("@amount", slot.amount),
                        new SqlParameter("@summonedHealth", slot.item.summonedHealth),
                        new SqlParameter("@summonedLevel", slot.item.summonedLevel),
                        new SqlParameter("@summonedExperience", slot.item.summonedExperience));
            }
        }
    }

    void SaveEquipment(Player player, MySqlCommand command)
    {
        // equipment: remove old entries first, then add all new ones
        // (we could use UPDATE where slot=... but deleting everything makes
        //  sure that there are never any ghosts)
        ExecuteNonQueryMySql(command, "DELETE FROM character_equipment WHERE `character`=@character", new SqlParameter("@character", player.name));
        for (int i = 0; i < player.equipment.Count; ++i)
        {
            ItemSlot slot = player.equipment[i];
            if (slot.amount > 0) // only relevant equip to save queries/storage/time
                ExecuteNonQueryMySql(command, "INSERT INTO character_equipment VALUES (@character, @slot, @name, @amount, @summonedHealth, @summonedLevel, @summonedExperience)",
                            new SqlParameter("@character", player.name),
                            new SqlParameter("@slot", i),
                            new SqlParameter("@name", slot.item.name),
                            new SqlParameter("@amount", slot.amount),
                            new SqlParameter("@summonedHealth", slot.item.summonedHealth),
                            new SqlParameter("@summonedLevel", slot.item.summonedLevel),
                            new SqlParameter("@summonedExperience", slot.item.summonedExperience));
        }
    }

    void SaveSkills(Player player, MySqlCommand command)
    {
        // skills: remove old entries first, then add all new ones
        ExecuteNonQueryMySql(command, "DELETE FROM character_skills WHERE `character`=@character", new SqlParameter("@character", player.name));
        foreach (var skill in player.skills)
        {
            // only save relevant skills to save a lot of queries and storage
            // (considering thousands of players)
            // => interesting only if learned or if buff/status (murderer etc.)
            if (skill.level > 0) // only relevant skills to save queries/storage/time
            {
                // castTimeEnd and cooldownEnd are based on Time.time, which
                // will be different when restarting the server, so let's
                // convert them to the remaining time for easier save & load
                // note: this does NOT work when trying to save character data shortly
                //       before closing the editor or game because Time.time is 0 then.
                ExecuteNonQueryMySql(command, @"
                    INSERT INTO character_skills 
                    SET
                        `character` = @character,
                        name = @name,
                        level = @level,
                        castTimeEnd = @castTimeEnd,
                        cooldownEnd = @cooldownEnd",
                        new SqlParameter("@character", player.name),
                        new SqlParameter("@name", skill.name),
                        new SqlParameter("@level", skill.level),
                        new SqlParameter("@castTimeEnd", skill.CastTimeRemaining()),
                        new SqlParameter("@cooldownEnd", skill.CooldownRemaining()));
            }
        }
    }

    void SaveBuffs(Player player, MySqlCommand command)
    {
        ExecuteNonQueryMySql(command, "DELETE FROM character_buffs WHERE `character`=@character", new SqlParameter("@character", player.name));
        foreach (var buff in player.buffs)
        {
            // buffTimeEnd is based on Time.time, which will be different when
            // restarting the server, so let's convert them to the remaining
            // time for easier save & load
            // note: this does NOT work when trying to save character data shortly
            //       before closing the editor or game because Time.time is 0 then.
            ExecuteNonQueryMySql(command, "INSERT INTO character_buffs VALUES (@character, @name, @level, @buffTimeEnd)",
                new SqlParameter("@character", player.name),
                new SqlParameter("@name", buff.name),
                new SqlParameter("@level", buff.level),
                new SqlParameter("@buffTimeEnd", buff.BuffTimeRemaining()));
        }
    }

    void SaveQuests(Player player, MySqlCommand command)
    {
        // quests: remove old entries first, then add all new ones
        ExecuteNonQueryMySql(command, "DELETE FROM character_quests WHERE `character`=@character", new SqlParameter("@character", player.name));
        foreach (var quest in player.quests)
        {
            ExecuteNonQueryMySql(command, "INSERT INTO character_quests VALUES (@character, @name, @field0, @completed)",
                new SqlParameter("@character", player.name),
                new SqlParameter("@name", quest.name),
                new SqlParameter("@progress", quest.progress),
                new SqlParameter("@completed", quest.completed));
        }
    }

    // adds or overwrites character data in the database
    void CharacterSave(Player player, bool online, MySqlCommand command)
    {
        var query = @"
            INSERT INTO characters 
            SET
                name=@name,
                account=@account,
                classname=@classname,
                x = @x,
                y = @y,
                z = @z,
                level = @level,
                health = @health,
                mana = @mana,
                strength = @strength,
                intelligence = @intelligence,
                experience = @experience,
                skillExperience = @skillExperience,
                gold = @gold,
                coins = @coins,
                online = @online,
                lastsaved = @lastsaved
            ON DUPLICATE KEY UPDATE 
                account=@account,
                classname = @classname,
                x = @x,
                y = @y,
                z = @z,
                level = @level,
                health = @health,
                mana = @mana,
                strength = @strength,
                intelligence = @intelligence,
                experience = @experience,
                skillExperience = @skillExperience,
                gold = @gold,
                coins = @coins,
                online = @online,
                lastsaved = @lastsaved
            ";

        ExecuteNonQueryMySql(command, query,
                    new SqlParameter("@name", player.name),
                    new SqlParameter("@account", player.account),
                    new SqlParameter("@classname", player.className),
                    new SqlParameter("@x", player.transform.position.x),
                    new SqlParameter("@y", player.transform.position.y),
                    new SqlParameter("@z", player.transform.position.z),
                    new SqlParameter("@level", player.level),
                    new SqlParameter("@health", player.health),
                    new SqlParameter("@mana", player.mana),
                    new SqlParameter("@strength", player.strength),
                    new SqlParameter("@intelligence", player.intelligence),
                    new SqlParameter("@experience", player.experience),
                    new SqlParameter("@skillExperience", player.skillExperience),
                    new SqlParameter("@gold", player.gold),
                    new SqlParameter("@coins", player.coins),
                    new SqlParameter("@online", online),
                    new SqlParameter("@lastsaved", DateTime.UtcNow)
                       );

        SaveInventory(player, command);
        SaveEquipment(player, command);
        SaveSkills(player, command);
        SaveBuffs(player, command);
        SaveQuests(player, command);

        // addon system hooks
        Utils.InvokeMany(typeof(Database), this, "CharacterSave_", player);
    }

    // adds or overwrites character data in the database
    public void CharacterSave(Player player, bool online, bool useTransaction = true)
    {
        // only use a transaction if not called within SaveMany transaction
        Transaction(command =>
        {
            CharacterSave(player, online, command);
        });
    }

    // save multiple characters at once (useful for ultra fast transactions)
    public void CharacterSaveMany(IEnumerable<Player> players, bool online = true)
    {
        Transaction(command =>
        {
            foreach (var player in players)
                CharacterSave(player, online, command);
        });
    }

    // guilds //////////////////////////////////////////////////////////////////
    public bool GuildExists(string guild)
    {
        return ((long)ExecuteScalarMySql("SELECT Count(*) FROM guild_info WHERE name=@name", new SqlParameter("@name", guild))) > 0;
    }

    Guild LoadGuild(string guildName)
    {
        Guild guild = new Guild();

        // set name
        guild.name = guildName;

        // load guild info
        var row = ExecuteDataRowMySql("SELECT * FROM guild_info WHERE name=@name", new SqlParameter("@name", guildName));
        if (row != null)
        {
            guild.notice = (string)row["notice"];
        }

        // load members list
        var members = new List<GuildMember>();

        using (var reader = GetReader("SELECT * FROM character_guild WHERE guild=@guild", new SqlParameter("@guild", guildName)))
        {
            while (reader.Read())
            {
                var member = new GuildMember();
                member.name = (string)reader["character"];
                member.rank = (GuildRank)((int)reader["rank"]);

                // is this player online right now? then use runtime data
                if (Player.onlinePlayers.TryGetValue(member.name, out Player player))
                {
                    member.online = true;
                    member.level = player.level;
                }
                else
                {
                    member.online = false;
                    var charRow = ExecuteDataRowMySql("SELECT * FROM characters WHERE name=@name", new SqlParameter("@name", member.name));
                    member.level = charRow != null ? (int)charRow["level"] : 1;
                }

                members.Add(member);
            };
        }

        guild.members = members.ToArray();
        return guild;
    }

    public void SaveGuild(Guild guild, List<GuildMember> members)
    {
        Transaction(command =>
        {
            var query = @"
                INSERT INTO guild_info
                SET
                    name = @guild,
                    notice = @notice
                ON DUPLICATE KEY UPDATE
                    notice = @notice";

            // guild info
            ExecuteNonQueryMySql(command, query,
                new SqlParameter("@guild", guild.name),
                new SqlParameter("@notice", guild.notice));

            // members list
            ExecuteNonQueryMySql(command, "DELETE FROM character_guild WHERE guild=@guild", new SqlParameter("@guild", guild.name));
            foreach (var member in members)
            {
                ExecuteNonQueryMySql(command, @"
                    INSERT INTO character_guild
                    SET
                        `character` = @character,
                        guild = @guild,
                        `rank` = @rank
                    ON DUPLICATE KEY UPDATE
                        `character` = @character,
                        guild = @guild,
                        `rank` = @rank",
                    new SqlParameter("@character", member.name),
                    new SqlParameter("@guild", guild.name),
                    new SqlParameter("@rank", member.rank));
            }
        });
    }

    public void RemoveGuild(string guild)
    {
        Transaction(command =>
        {
            ExecuteNonQueryMySql(command, "DELETE FROM guild_info WHERE name=@name", new SqlParameter("@name", guild));
            ExecuteNonQueryMySql(command, "DELETE FROM character_guild WHERE guild=@name", new SqlParameter("@name", guild));
        });
    }

    // item mall ///////////////////////////////////////////////////////////////
    public List<long> GrabCharacterOrders(string characterName)
    {
        // grab new orders from the database and delete them immediately
        //
        // note: this requires an orderid if we want someone else to write to
        // the database too. otherwise deleting would delete all the new ones or
        // updating would update all the new ones. especially in sqlite.
        //
        // note: we could just delete processed orders, but keeping them in the
        // database is easier for debugging / support.
        var result = new List<long>();
        var table = ExecuteReaderMySql("SELECT orderid, coins FROM character_orders WHERE `character`=@character AND processed=0", new SqlParameter("@character", characterName));
        foreach (var row in table)
        {
            result.Add((long)row[1]);
            ExecuteNonQueryMySql("UPDATE character_orders SET processed=1 WHERE orderid=@orderid", new SqlParameter("@orderid", (long)row[0]));
        }

        return result;
    }
}
