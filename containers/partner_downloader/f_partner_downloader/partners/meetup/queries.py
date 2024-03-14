past_events_query = """query GetGroupPastEvents($urlname: String!) {
  groupByUrlname(urlname: $urlname) {
    name
    pastEvents(input: {}, filter: {}, sortOrder: DESC) {
      edges {
        node {
          id
          title
          description
          venue {
            name
            address
            city
            country
            lat
            lng
          }
          eventUrl
          dateTime
          going
          image {
            source
          }
        }
      }
    }
  }
}
"""

curr_events_query = """query GetGroupPastEvents($urlname: String!) {
  groupByUrlname(urlname: $urlname) {
    name
    upcomingEvents(input: {}, filter: {}, sortOrder: DESC) {
      edges {
        node {
          id
          title
          description
          venue {
            name
            address
            city
            country
            lat
            lng
          }
          eventUrl
          dateTime
          going
          image {
            source
          }
        }
      }
    }
  }
}
"""
