document.addEventListener("DOMContentLoaded", function(event) {

  var eg = {
    '1': [11, 12, 13, 14, 15],
    '2': [21, 22, 23, 24, 25],
    '3': [31, 32, 33, 34, 35],
    '4': [41, 42, 43, 44, 45]
  }

  $('.search-icon').click(function() {
    handleInput()
  });

  $('#input-box').keypress(function onEnter(e) {
    if (e.which == 13) {
      handleInput()
    }
  })

  function handleInput() {
    var ip_box = $('#input-box');
    var input = ip_box.val(); // Get the input value
    if (input != '') {
      output = { 'data': input }
      /*$.post('/data', JSON.stringify(output), function success(returnedData, status, resp) {
        // Modify page
        console.log(returnedData)
        populateData(returnedData)
        $('.loader').addClass('hidden')
      }, 'json');*/
      $.ajax({
        type: "POST",
        url: "/data",
        data: JSON.stringify(output),
        timeout: 20000,
        success: function(returnedData) {
            // Modify page
            console.log(returnedData['data'])
            populateData(returnedData['data'])
            $('.loader').addClass('hidden')
        }
      });
      // Show loading
      $('.loader').removeClass('hidden');
    }
  }

  function populateData(data) {
    var string_template = `
    <li class="list-group-item">
      <div class="panel panel-success">
        <div class="panel-heading">
          <h3 class="panel-title">Movie Title  <span class="badge">Rating</span></h3>
        </div>
        <div class="panel-body">
          Extra Information
        </div>
      </div>
    </li>`
    $.each(data, function(index_main, val) {
      $('.lg-' + index_main).empty();
      $.each(val, function(index, info) {
        console.log(info)
        title = info['title']
        genres = info['genres']
        director = info['director']
        actors = info['cast']
        ratings = info['rating']
        info_str = "Genres: " + genres + "<br>"
        info_str += "Director: " + director + "<br>"
        info_str += "Cast: " + actors + "<br>"
        var res = string_template.replace('Movie Title', title)
        var res = res.replace('Rating', ratings.toPrecision(2) + '★')
        var res = res.replace('Extra Information', info_str)
        $('.lg-' + index_main).append(res);
      })
    })
  }

});
