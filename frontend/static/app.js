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
      $.post('/data', JSON.stringify(output), function success(returnedData, status, resp) {
        // Modify page
        populateData(returnedData)
        $('.loader').addClass('hidden')
      }, 'json');
      // Show loading
      $('.loader').removeClass('hidden');
    }
  }

  function populateData(data) {
    var string_template = `
    <li class="list-group-item">
      <div class="panel panel-success">
        <div class="panel-heading">
          <h3 class="panel-title">Movie Title</h3>
        </div>
        <div class="panel-body">
          Extra Information
        </div>
        <img src="http://dummyimage.com/80x120/000/fff">
      </div>
    </li>`
    $.each(eg, function(k_main, v_main) {
      $('.lg-' + k_main).empty();
      $.each(eg[k_main], function(index, val) {
        var res = string_template.replace('Movie Title', val)
        var res = res.replace('Extra Information', k_main)
        $('.lg-' + k_main).append(res);
      })
    })
  }

});
