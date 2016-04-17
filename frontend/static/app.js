document.addEventListener("DOMContentLoaded", function(event) {
  setInterval(
    function() {
      $.getJSON('/data', {}, function(data) {
        // Do Stuff
        console.log(data["data"]);
        $('.panel-body').each(function editData() {
          $(this).text(data["data"])
        })
      });
    }, 1000);
});
