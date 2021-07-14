using Microsoft.Extensions.Configuration;
using NRedi2Read.Helpers;
using NRedi2Read.Models;
using NRediSearch;
using NRediSearch.QueryBuilder;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NRedi2Read.Services
{
    public class UserService
    {
        private const string USER_INDEX_NAME = "user-idx";
        private readonly IDatabase _db;
        private readonly Client _searchClient;
        private readonly int _bcryptWorkFactory;

        public UserService(IConnectionMultiplexer multiplexer, IConfiguration config)
        {
            _db = multiplexer.GetDatabase();
            _searchClient = new Client(USER_INDEX_NAME, _db);
            if(config["BCryptWorkFactor"] != null)
            {
                _bcryptWorkFactory = int.Parse(config["BCryptWorkFactor"]); // use a differnet work factor
            }
            else
            {
                _bcryptWorkFactory = 11; // use the default
            }

        }
        
        /// <summary>
        /// Creates a user
        /// </summary>
        /// <param name="user"></param>
        /// <returns></returns>
        public async Task Create(User user)
        {            
            user.Password = BCrypt.Net.BCrypt.HashPassword(user.Password);
            if (user.Books!=null)
            {
                await _db.SetAddAsync(UserBookKey(user.Id), user.Books.Select(r => new RedisValue(r.ToString())).ToArray());
            }            
            user.Books = null;
            await _db.HashSetAsync(UserKey(user.Id), user.AsHashEntries().ToArray());
        }

        /// <summary>
        /// adds a range of books to the users book id
        /// </summary>
        /// <param name="id"></param>
        /// <param name="books"></param>
        /// <returns></returns>
        public async Task AddBooks(string id, params string[] books)
        {            
            await _db.SetAddAsync(UserBookKey(id), books.Select(b=>new RedisValue(b)).ToArray());
        }

        /// <summary>
        /// Creates a set of user NOTE this can take a very long time if you don't set the
        /// BCryptWorkFactor configuration item to a lower number (e.g. 4) which is fine for demonstration
        /// but obviously you'd want to avoid using a lower work factor for any production item as the password
        /// hashes are intrinsically more vulnerable.
        /// </summary>
        /// <param name="users"></param>
        /// <returns></returns>
        public async Task CreateBulk(IEnumerable<User> users)
        {            
            var tasks = new List<Task>();
            foreach(var user in users)
            {
                user.Password = BCrypt.Net.BCrypt.HashPassword(user.Password, _bcryptWorkFactory);
                if (user.Books != null)
                {
                    tasks.Add(_db.SetAddAsync(UserBookKey(user.Id), user.Books.Select(r => new RedisValue(r.ToString())).ToArray()));
                }
                tasks.Add(_db.HashSetAsync(UserKey(user.Id), user.AsHashEntries().ToArray()));
            }
            await Task.WhenAll(tasks);
        }

        /// <summary>
        /// Checks the existence of users for given IDs
        /// </summary>
        /// <param name="ids"></param>
        /// <returns></returns>
        public async Task<IEnumerable<string>> CheckBulk(IEnumerable<string> ids)
        {            
            var tasks = new List<Task<bool>>();
            foreach (var id in ids)
            {
                tasks.Add(_db.KeyExistsAsync(UserKey(id)));
            }
            await Task.WhenAll(tasks);
            return ids.Where((id, index) => tasks[index].Result);
        }

        /// <summary>
        /// gets a single user from the database
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<User> Read(string id)
        {
            var result = await _db.HashGetAllAsync(UserKey(id));
            var user = RedisHelper.ConvertFromRedis<User>(result);
            var books = await _db.SetMembersAsync(UserBookKey(id));
            user.Books = books.Select(r=>r.ToString()).ToList();
            return user;
        }

        /// <summary>
        /// generates a redis key for a user id
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public static RedisKey UserKey(string id)
        {
            return new(new User().GetType().Name + ":" + id);
        }

        public static RedisKey UserBookKey(string id)
        {
            return new RedisKey($"{UserKey(id)}:Books");
        }

        public void CreateUserIndex()
        {
            // Add Index Creation Logic
        }

        public async Task<User> ValidateUserCredentials(string email, string password)
        {
            // Add validation logic!
            return null;
        }

        public async Task<User> GetUserWithEmail(string email)
        {
            return null;
        }

    }
}