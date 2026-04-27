using MatchLogic.Application.Common;
using MatchLogic.Application.Core;
using MatchLogic.Domain.Dictionary;
using System.Collections.Generic;

namespace MatchLogic.Application.Core;

public class DictionaryCategorySeedProvider : DataSeedProviderBase<DictionaryCategory>
{
    public override string GetCollectionName() => Constants.Collections.DictionaryCategory;

    public override IEnumerable<DictionaryCategory> GetSeedData()
    {
        var categories = new List<DictionaryCategory>
        {
            // US States Dictionary
            new DictionaryCategory
            {
                Name = "US States",
                Description = "List of US states and territories",
                Items = new List<string>
                {
                    "Alabama", "Alaska", "Arizona", "Arkansas", "California",
                    "Colorado", "Connecticut", "Delaware", "Florida", "Georgia",
                    "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa",
                    "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland",
                    "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri",
                    "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey",
                    "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
                    "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina",
                    "South Dakota", "Tennessee", "Texas", "Utah", "Vermont",
                    "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming",
                    "District of Columbia", "American Samoa", "Guam", "Northern Mariana Islands",
                    "Puerto Rico", "U.S. Virgin Islands"
                },
                IsSystem = true,
                IsDeleted = false,
                Version = 1
            },

            // US Cities Dictionary
            new DictionaryCategory
            {
                Name = "US Cities",
                Description = "List of common US cities",
                Items = new List<string>
                {
                    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
                    "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
                    "Austin", "Jacksonville", "Fort Worth", "Columbus", "Indianapolis",
                    "Charlotte", "San Francisco", "Seattle", "Denver", "Washington DC",
                    "Boston", "El Paso", "Nashville", "Detroit", "Oklahoma City",
                    "Portland", "Las Vegas", "Memphis", "Louisville", "Baltimore",
                    "Milwaukee", "Albuquerque", "Tucson", "Fresno", "Sacramento",
                    "Mesa", "Kansas City", "Atlanta", "Long Beach", "Colorado Springs",
                    "Raleigh", "Miami", "Omaha", "Minneapolis", "Tulsa",
                    "Cleveland", "Wichita", "Arlington", "New Orleans", "Bakersfield",
                    "Tampa", "Honolulu", "Aurora", "Anaheim", "Santa Ana",
                    "St. Louis", "Riverside", "Corpus Christi", "Lexington", "Pittsburgh",
                    "Anchorage", "Stockton", "Cincinnati", "St. Paul", "Toledo",
                    "Newark", "Greensboro", "Plano", "Henderson", "Lincoln",
                    "Buffalo", "Jersey City", "Chula Vista", "Fort Wayne", "Orlando",
                    "St. Petersburg", "Chandler", "Laredo", "Norfolk", "Durham",
                    "Madison", "Lubbock", "Irvine", "Winston-Salem", "Glendale",
                    "Garland", "Hialeah", "Reno", "Chesapeake", "Gilbert",
                    "Baton Rouge", "Irving", "Scottsdale", "North Las Vegas", "Fremont",
                    "Boise", "Richmond", "San Bernardino", "Birmingham", "Spokane"
                },
                IsSystem = true,
                IsDeleted = false,
                Version = 1
            },

            // First Names Dictionary
            new DictionaryCategory
            {
                Name = "First Names",
                Description = "List of common first names",
                Items = new List<string>
                {
                    // Male first names
                    "James", "John", "Robert", "Michael", "William",
                    "David", "Richard", "Joseph", "Thomas", "Charles",
                    "Christopher", "Daniel", "Matthew", "Anthony", "Mark",
                    "Donald", "Steven", "Paul", "Andrew", "Joshua",
                    "Kenneth", "Kevin", "Brian", "George", "Timothy",
                    "Ronald", "Edward", "Jason", "Jeffrey", "Ryan",
                    "Jacob", "Gary", "Nicholas", "Eric", "Jonathan",
                    "Stephen", "Larry", "Justin", "Scott", "Brandon",
                    "Benjamin", "Samuel", "Gregory", "Alexander", "Frank",
                    "Patrick", "Raymond", "Jack", "Dennis", "Jerry",
                    
                    // Female first names
                    "Mary", "Patricia", "Jennifer", "Linda", "Elizabeth",
                    "Barbara", "Susan", "Jessica", "Sarah", "Karen",
                    "Lisa", "Nancy", "Betty", "Margaret", "Sandra",
                    "Ashley", "Kimberly", "Emily", "Donna", "Michelle",
                    "Carol", "Amanda", "Dorothy", "Melissa", "Deborah",
                    "Stephanie", "Rebecca", "Sharon", "Laura", "Cynthia",
                    "Kathleen", "Amy", "Shirley", "Angela", "Helen",
                    "Anna", "Brenda", "Pamela", "Nicole", "Emma",
                    "Samantha", "Katherine", "Christine", "Debra", "Rachel",
                    "Catherine", "Carolyn", "Janet", "Ruth", "Maria",
                    
                    // Gender-neutral names
                    "Taylor", "Jordan", "Casey", "Riley", "Jessie",
                    "Jackie", "Avery", "Jaime", "Peyton", "Kerry",
                    "Jody", "Kendall", "Morgan", "Reese", "Jamie",
                    "Dana", "Quinn", "Blake", "Drew", "Parker"
                },
                IsSystem = true,
                IsDeleted = false,
                Version = 1
            },

            // Last Names Dictionary
            new DictionaryCategory
            {
                Name = "Last Names",
                Description = "List of common last names",
                Items = new List<string>
                {
                    "Smith", "Johnson", "Williams", "Brown", "Jones",
                    "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
                    "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
                    "Thomas", "Taylor", "Moore", "Jackson", "Martin",
                    "Lee", "Perez", "Thompson", "White", "Harris",
                    "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
                    "Walker", "Young", "Allen", "King", "Wright",
                    "Scott", "Torres", "Nguyen", "Hill", "Flores",
                    "Green", "Adams", "Nelson", "Baker", "Hall",
                    "Rivera", "Campbell", "Mitchell", "Carter", "Roberts",
                    "Gomez", "Phillips", "Evans", "Turner", "Diaz",
                    "Parker", "Cruz", "Edwards", "Collins", "Reyes",
                    "Stewart", "Morris", "Morales", "Murphy", "Cook",
                    "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper",
                    "Peterson", "Bailey", "Reed", "Kelly", "Howard",
                    "Ramos", "Kim", "Cox", "Ward", "Richardson",
                    "Watson", "Brooks", "Chavez", "Wood", "James",
                    "Bennett", "Gray", "Mendoza", "Ruiz", "Hughes",
                    "Price", "Alvarez", "Castillo", "Sanders", "Patel",
                    "Myers", "Long", "Ross", "Foster", "Jimenez",
                    "Powell", "Jenkins", "Perry", "Russell", "Sullivan"
                },
                IsSystem = true,
                IsDeleted = false,
                Version = 1
            }
        };

        return categories;
    }
}
