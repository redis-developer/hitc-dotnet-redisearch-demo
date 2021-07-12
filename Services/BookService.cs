using NRedi2Read.Helpers;
using NRedi2Read.Models;
using NRediSearch;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace NRedi2Read.Services
{
    public class BookService
    {
        private const string BOOK_INDEX_NAME = "books-idx";
        private readonly IDatabase _db;
        private readonly Client _searchClient;

        public BookService(IConnectionMultiplexer multiplexer)
        {
            _db = multiplexer.GetDatabase();
            _searchClient = new Client(BOOK_INDEX_NAME, _db);
        }

        /// <summary>
        /// Get a single book
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<Book> Get(string id)
        {
            var query = new Query($"@id:{{{id}}}");
            var result = await _searchClient.SearchAsync(query);
            if(result.TotalResults == 0)
            {
                return null;
            }
            return result.AsList<Book>().FirstOrDefault();
        }

        /// <summary>
        /// Creates a whole set of books in the database, great for bulk-loading when
        /// the app is starting up
        /// </summary>
        /// <param name="books"></param>
        /// <returns></returns>
        public async Task<bool> CreateBulk(IEnumerable<Book> books)
        {
            var tasks = new List<Task>();
            foreach(var book in books)
            {
                var hashEntries = book.AsHashEntries().ToArray();
                tasks.Add(_db.HashSetAsync(BookKey(book.Id), hashEntries));
            }
            await Task.WhenAll(tasks.ToArray());

            return true;
        }

        /// <summary>
        /// creates a single book
        /// </summary>
        /// <param name="book"></param>
        /// <returns></returns>
        public async Task<Book> Create(Book book)
        {            
            _db.HashSet(BookKey(book.Id), book.AsHashEntries().ToArray());
            return await Get(book.Id);
        }

        /// <summary>
        /// Get all the books associated with the given Ids
        /// </summary>
        /// <param name="ids"></param>
        /// <returns></returns>
        public async Task<IEnumerable<string>> GetBulk(IEnumerable<string> ids)
        {            
            var tasks = new List<Task<RedisValue>>();
            foreach(var id in ids)
            {
                tasks.Add(_db.HashGetAsync(BookKey(id), "id"));
            }
            await Task.WhenAll(tasks);
            return tasks.Select(t => t.Result.ToString());
        }

        /// <summary>
        /// Searches for books matching the given query see the
        /// <see href="https://oss.redislabs.com/redisearch/Commands/#ftsearch">RediSearch</see>
        /// docs for details on how to structure queries.
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        public async Task<IEnumerable<Book>> Search(string query, string sortBy, string direction)
        {
            var q = new Query(query);
            q.SortBy = sortBy;
            q.SortAscending = direction == "ASC";
            var result = await _searchClient.SearchAsync(q);
            return result.AsList<Book>();
        }

        /// <summary>
        /// Paginates all the books in the database for the given query.
        /// </summary>
        /// <param name="query"></param>
        /// <param name="page"></param>
        /// <param name="pageSize"></param>
        /// <returns></returns>
        public async Task<IList<Book>> PaginateBooks(string query, int page, string sortBy="title", string direction="ASC", int pageSize = 10)
        {
            var q = new Query(query);
            q.SortBy = sortBy;
            q.SortAscending = direction == "ASC";
            q.Limit(page * pageSize, pageSize);
            var results = await _searchClient.SearchAsync(q);
            return results.AsList<Book>();
        }

        /// <summary>
        /// Creates the book index
        /// </summary>
        /// <returns></returns>
        public async Task CreateBookIndex()
        {
            // drop the index, if it doesn't exists, that's fine
            try
            {
                await _db.ExecuteAsync("FT.DROPINDEX", "books-idx");
            }
            catch(Exception)
            {
                // books-idx didn't exist - don't do anything
            }

            var schema = new Schema();

            schema.AddSortableTextField("title");
            schema.AddTextField("subtitle");
            schema.AddTextField("description");
            schema.AddSortableNumericField("price");
            schema.AddTagField("id");
            schema.AddTextField("authors.[0]");
            schema.AddTextField("authors.[1]");
            schema.AddTextField("authors.[2]");
            schema.AddTextField("authors.[3]");
            schema.AddTextField("authors.[4]");
            schema.AddTextField("authors.[5]");
            schema.AddTextField("authors.[7]");
            var options = new Client.ConfiguredIndexOptions(
                new Client.IndexDefinition( prefixes: new [] { "Book:" } )
            );
            await _searchClient.CreateIndexAsync(schema, options);
        }
        
        /// <summary>
        /// generates a redis key for a book of a given id
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private static RedisKey BookKey(string id)
        {
            return new RedisKey(new Book().GetType().Name + ":" + id);
        }
    }
}