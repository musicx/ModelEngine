$(document).ready(function(){
    $('a.goto_project').click(function(){
        var $body = (window.opera) ? (document.compatMode == "CSS1Compat" ? $('html') : $('body')) : $('html,body');
        $body.animate({
            scrollTop: $('#project').offset().top - 170
        }, 1000);
        return false;
    });
    $('a.goto_preproc').click(function(){
        var $body = (window.opera) ? (document.compatMode == "CSS1Compat" ? $('html') : $('body')) : $('html,body');
        $body.animate({
            scrollTop: $('#pre_processing').offset().top - 30
        }, 1000);
        return false;
    });
    $('a.goto_sas').click(function(){
        var $body = (window.opera) ? (document.compatMode == "CSS1Compat" ? $('html') : $('body')) : $('html,body');
        $body.animate({
            scrollTop: $('#sas').offset().top - 30
        }, 1000);
        return false;
    });
    $('a.goto_nnmodel').click(function(){
        var $body = (window.opera) ? (document.compatMode == "CSS1Compat" ? $('html') : $('body')) : $('html,body');
        $body.animate({
            scrollTop: $('#neuralnetwork').offset().top - 30
        }, 1000);
        return false;
    });
    $('a.goto_treemodel').click(function(){
        var $body = (window.opera) ? (document.compatMode == "CSS1Compat" ? $('html') : $('body')) : $('html,body');
        $body.animate({
            scrollTop: $('#treenet').offset().top - 30
        }, 1000);
        return false;
    });
    $('a.goto_custom').click(function(){
        var $body = (window.opera) ? (document.compatMode == "CSS1Compat" ? $('html') : $('body')) : $('html,body');
        $body.animate({
            scrollTop: $('#custom').offset().top - 30
        }, 1000);
        return false;
    });
    $('a.goto_evaluate').click(function(){
        var $body = (window.opera) ? (document.compatMode == "CSS1Compat" ? $('html') : $('body')) : $('html,body');
        $body.animate({
            scrollTop: $('#evaluation').offset().top - 30
        }, 1000);
        return false;
    });
});
