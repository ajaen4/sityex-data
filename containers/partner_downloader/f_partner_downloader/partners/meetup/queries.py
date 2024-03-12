get_group_query = """query GetGroupPastEvents($urlname: String!) {
  groupByUrlname(urlname: $urlname) {
    name
    pastEvents(input: {}, filter: {}, sortOrder: DESC) {
      edges {
        node {
          title
          description
          venue {
            name
            address
            city
            state
            country
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
