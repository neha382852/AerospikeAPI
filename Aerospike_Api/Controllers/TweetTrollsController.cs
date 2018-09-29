using Aerospike.Client;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace Aerospike_Api.Controllers
{
    public class TweetTrollsController : ApiController
    {
        AerospikeClient aerospikeClient = new AerospikeClient("18.235.70.103", 3000);
        string nameSpace = "AirEngine";
        string setName = "Neha";
        [HttpPut]
        [Route("GetDataByListOfId")]
        public List<Record> GetDataById([FromBody]List<string> tweets)
        {
            List<Record> tweetcontent = new List<Record>();
            foreach (var tweet in tweets)
            {
                try
                {
                    Record record = aerospikeClient.Get(new BatchPolicy(), new Key(nameSpace, setName, tweet));
                    if (record != null)
                        tweetcontent.Add(record);
                    else
                        throw new Exception("No Such Id exists");
                }
                catch(Exception exception)
                {
                    return null;
                }
            }
            return tweetcontent;
        }

        [HttpDelete]
        [Route("DeleteData/{tweet_id}")]
        public string DeleteDataById([FromUri]string tweet_id)
        {
            var key = new Key(nameSpace, setName, tweet_id);
            if (key != null)
            {
                aerospikeClient.Delete(new WritePolicy(), key);
                return "Data Successfully deleted";
            }
            else
            {
                return "No such id exists";
            }
        }


        /* Example of jsonobject in postman {
	                "tweet_id":"816533332318191617",
	                "binName":"author",
	                "updatedvalue":"neha"
                    }*/ 
        [HttpPut]
        [Route("UpdateData")]
        public string UpdateDataById([FromBody]JObject input)
        {
                    string tweet_id = input.GetValue("tweet_id").ToString();
                    string binName = input.GetValue("binName").ToString();
                    string UpdatedValue = input.GetValue("updatedvalue").ToString();
                    Bin newBin = new Bin(binName, UpdatedValue);
                    var key = new Key(nameSpace, setName, tweet_id);
                    aerospikeClient.Put(new WritePolicy(), key, newBin);
                    return "Data successfully updated";
        }
    }
}
