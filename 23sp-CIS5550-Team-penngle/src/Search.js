
// function search(form) {
//   const query = form.q.value.toLowerCase();

//   // Load the contents of fake.json
//   fetch('fake.json') // /query/:url  (GET/POST)
//                       // url: ..... (POST)
//     .then(response => response.json())
//     .then(data => {
//       // Filter the data based on the search query
//       const filteredData = data.filter(item => {
//         const title = item.title ? item.title.toLowerCase() : '';
//         const content = item.content ? item.content.toLowerCase() : '';
//         return title.includes(query) || content.includes(query);
//       });

//       // Display the filtered results
//       displayResults(filteredData, 1, 10);
//     })
//     .catch(error => console.error(error));

//   return false;
// }

// function search(form) {
//   const query = form.q.value.toLowerCase();
  
//   fetch(`/query?query=${query}`)
//     .then(response => response.text())
//     .then(response => {
//       const data = JSON.parse(response);
//       // console.log(data);
//       return data;
//     })
//     .then(urls => {
//       const results = [];
//       // Loop through each URL, make a GET request to the backend to retrieve its information, and add it to the results array
//       urls.forEach(url => {
//         fetch(`/query/${url.url}`)
//           .then(response => response.text())
//           .then(response => {
//             const data = JSON.parse(response);
//             console.log(data);
//             return data;
//           })
//           .then(data => {
//             results.push({
//               title: data.title,
//               url: data.url,
//               description: data.description
//             });

//             displayResults(results, 1, 10);
//           })
//           .catch(error => console.error(error));
//       });
//     })
//     .catch(error => console.error(error));
//   return false;
// }

function search(form) {
  const query = form.q.value.toLowerCase();

  fetch(`/query?query=${query}`)
    .then(response => response.text())
    .then(response => {
      const data = JSON.parse(response);
      return data;
    })
    .then(urls => {
      const promises = urls.map(url => {
        return fetch(`/query/${url.url}`)
          .then(response => response.json())
          .then(data => ({
            title: data.title,
            url: data.url,
            description: data.description
          }));
      });

      return Promise.all(promises);
    })
    .then(results => {
      displayResults(results, 1, 10);
    })
    .catch(error => console.error(error));

  return false;
}


// function displayResults(results, page, pageSize) {
//   const startIndex = (page - 1) * pageSize;
//   const endIndex = startIndex + pageSize;
//   const paginatedResults = results.slice(startIndex, endIndex);

//   const resultsDiv = document.getElementById('results');
//   resultsDiv.innerHTML = '';

//   if (paginatedResults.length === 0) {
//     resultsDiv.textContent = 'No results found.';
//   } else {
//     const ul = document.createElement('ul');
//     paginatedResults.forEach((result) => {
//       console.log("result:  "+result.url);
//       const li = document.createElement('li');

//       // Title
//       const title = document.createElement('h4');
//       const a = document.createElement('a');
//       a.href = result.url;
//       a.textContent = result.title;
//       title.appendChild(a);
//       li.appendChild(title);

//       // Description
//       const description = document.createElement('p');
//       description.textContent = result.description;
//       li.appendChild(description);

//       ul.appendChild(li);
//     });
//     resultsDiv.appendChild(ul);

//     if (results.length > pageSize) {
//       const numPages = Math.ceil(results.length / pageSize);
//       const paginationDiv = document.createElement('div');
//       paginationDiv.classList.add('pagination');
//       for (let i = 1; i <= numPages; i++) {
//         const button = document.createElement('button');
//         button.textContent = i;
//         button.addEventListener('click', () => {
//           displayResults(results, i, pageSize);
//         });
//         if (i === page) {
//           button.classList.add('active');
//         }
//         paginationDiv.appendChild(button);
//       }
//       resultsDiv.appendChild(paginationDiv);
//     }
//   }
// }

function displayResults(results, page, pageSize) {
  const startIndex = (page - 1) * pageSize;
  const endIndex = startIndex + pageSize;
  const paginatedResults = results.slice(startIndex, endIndex);

  const resultsDiv = document.getElementById('results');
  resultsDiv.innerHTML = '';

  if (paginatedResults.length === 0) {
    resultsDiv.textContent = 'No results found.';
  } else {
    const ul = document.createElement('ul');
    paginatedResults.forEach((result) => {
      const li = document.createElement('li');

      // Title
      const title = document.createElement('h4');
      const a = document.createElement('a');
      a.href = result.url;
      // a.target = _blank;
      a.textContent = result.title;
      title.appendChild(a);
      li.appendChild(title);

      // Description
      const description = document.createElement('p');
      description.textContent = result.description;
      // description.href = "localhost:8001/data/plain_text/" + result.url
      li.appendChild(description);

      ul.appendChild(li);
    });
    resultsDiv.appendChild(ul);

    if (results.length > pageSize) {
      const numPages = Math.ceil(results.length / pageSize);
      const paginationContainer = document.createElement('div');
      paginationContainer.classList.add('pagination-container');

      const paginationDiv = document.createElement('div');
      paginationDiv.classList.add('pagination');
      for (let i = 1; i <= numPages; i++) {
        const button = document.createElement('button');
        button.textContent = i;
        button.addEventListener('click', () => {
          displayResults(results, i, pageSize);
        });
        if (i === page) {
          button.classList.add('active');
        }
        paginationDiv.appendChild(button);
      }
      paginationContainer.appendChild(paginationDiv);

      // Add "Next Page" button
      const nextPageButton = document.createElement('button');
      nextPageButton.textContent = 'Next Page';
      nextPageButton.classList.add('next-page-btn');
      nextPageButton.addEventListener('click', () => {
        if (page < numPages) {
          displayResults(results, page + 1, pageSize);
        }
      });
      paginationContainer.appendChild(nextPageButton);

      resultsDiv.appendChild(paginationContainer);
    }
  }
}

